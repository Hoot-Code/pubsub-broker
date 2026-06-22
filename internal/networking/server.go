// Package networking implements the TCP server and connection management.
package networking

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
)

// Handler processes all frames from a single client connection.
// It is called synchronously in a per-connection goroutine.
type Handler interface {
	Handle(ctx context.Context, conn *Conn) error
}

// ─── Server ──────────────────────────────────────────────────────────────────

// Server is a TCP server that accepts connections and dispatches frames.
// When TLSCertFile and TLSKeyFile are set in the NetworkConfig, all connections
// are wrapped with TLS. If the certificate files are configured but cannot be
// loaded, Start returns an error rather than silently falling back to plaintext.
type Server struct {
	cfg     *config.NetworkConfig
	handler Handler
	log     *logging.Logger
	// listenerVal holds a net.Listener; use atomicListener()/storeListener()
	// accessors so concurrent reads from Addr() are race-free.
	listenerVal atomic.Value
	connCount   atomic.Int64
	started     atomic.Bool
	draining    atomic.Bool // set by SetDraining to stop accepting new conns
	mu          sync.Mutex
	conns       map[*Conn]struct{}
	stopC       chan struct{}
	stopped     chan struct{}
}

// NewServer creates a TCP server.
func NewServer(cfg *config.NetworkConfig, handler Handler, log *logging.Logger) *Server {
	return &Server{
		cfg:     cfg,
		handler: handler,
		log:     log,
		conns:   make(map[*Conn]struct{}),
		stopC:   make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

// Start begins accepting connections. Blocks until Close is called.
// If TLSCertFile is non-empty, the listener is wrapped with TLS using
// the provided certificate and key. A minimum TLS version of TLS 1.3 is
// enforced unless TLSMinVersion is explicitly set.
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	// Optionally wrap with TLS.
	if s.cfg.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
		if err != nil {
			_ = ln.Close()
			return fmt.Errorf("server: load tls keypair (%s, %s): %w",
				s.cfg.TLSCertFile, s.cfg.TLSKeyFile, err)
		}
		minVer := s.cfg.TLSMinVersion
		if minVer == 0 {
			minVer = tls.VersionTLS13
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   minVer,
		}
		ln = tls.NewListener(ln, tlsCfg)
		if s.log != nil {
			s.log.Info("tls enabled", "cert", s.cfg.TLSCertFile, "min_version", minVer)
		}
	}

	s.storeListener(ln)
	s.started.Store(true)
	if s.log != nil {
		s.log.Info("tcp server listening", "addr", ln.Addr())
	}

	go s.acceptLoop(ln)
	<-s.stopC
	_ = ln.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.close()
	}
	s.mu.Unlock()
	close(s.stopped)
	return nil
}

// storeListener atomically stores the listener so Addr() is race-free.
func (s *Server) storeListener(ln net.Listener) { s.listenerVal.Store(ln) }

// atomicListener returns the stored listener, or nil.
func (s *Server) atomicListener() net.Listener {
	v := s.listenerVal.Load()
	if v == nil {
		return nil
	}
	return v.(net.Listener)
}

// Close shuts down the server gracefully.
// Safe to call even if Start was never called.
func (s *Server) Close() error {
	if !s.started.Load() {
		return nil
	}
	select {
	case <-s.stopC:
	default:
		close(s.stopC)
	}
	<-s.stopped
	return nil
}

// SetDraining controls whether the server stops accepting new connections.
// When draining is true, Accept calls are still made (to allow the OS to
// complete the TCP handshake) but the resulting connections are immediately
// closed. Existing connections are kept alive so in-flight requests can
// complete.  Used by the graceful-stop sequence (D2).
func (s *Server) SetDraining(v bool) { s.draining.Store(v) }

// IsDraining reports whether the server is in draining mode.
func (s *Server) IsDraining() bool { return s.draining.Load() }

// ActiveConnections returns the number of open connections.
func (s *Server) ActiveConnections() int64 { return s.connCount.Load() }

// Addr returns the network address the server is listening on.
// Returns nil if the server has not yet started.
func (s *Server) Addr() net.Addr {
	ln := s.atomicListener()
	if ln == nil {
		return nil
	}
	return ln.Addr()
}

func (s *Server) acceptLoop(ln net.Listener) {
	var backoff time.Duration = 5 * time.Millisecond
	for {
		select {
		case <-s.stopC:
			return
		default:
		}

		raw, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopC:
				return
			default:
			}
			ne, ok := err.(net.Error)
			if ok && ne.Timeout() {
				time.Sleep(backoff)
				if backoff < time.Second {
					backoff *= 2
				}
				continue
			}
			if s.log != nil {
				s.log.Error("accept error", "err", err)
			}
			return
		}
		backoff = 5 * time.Millisecond

		// During drain we close the new connection immediately so that no
		// new requests can start while we wait for in-flight ones to finish.
		if s.draining.Load() {
			_ = raw.Close()
			continue
		}

		if s.connCount.Load() >= int64(s.cfg.MaxConnections) {
			if s.log != nil {
				s.log.Warn("max connections reached; rejecting", "max", s.cfg.MaxConnections)
			}
			_ = raw.Close()
			continue
		}

		// C1: Disable Nagle's algorithm so ACK frames are sent immediately
		// without waiting for the OS to coalesce them with subsequent writes.
		if tc, ok := raw.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}

		conn := newConn(raw, s.cfg)
		s.trackConn(conn)
		s.connCount.Add(1)

		go func() {
			defer func() {
				conn.close()
				s.untrackConn(conn)
				s.connCount.Add(-1)
			}()
			if err := s.handler.Handle(context.Background(), conn); err != nil {
				if s.log != nil {
					s.log.Debug("connection closed", "remote", raw.RemoteAddr(), "err", err)
				}
			}
		}()
	}
}

func (s *Server) trackConn(c *Conn) {
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) untrackConn(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}
