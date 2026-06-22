// Package client implements a high-level pub/sub broker client.
package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	proto "github.com/Hoot-Code/pubsub-broker/pkg/protocol"
)

// Client is a thread-safe connection to a single broker node.
// Obtain one with Dial.
type Client struct {
	opts clientOptions

	mu      sync.Mutex
	conn    net.Conn
	enc     *proto.Encoder
	dec     *proto.Decoder
	closed  atomic.Bool
	closeCh chan struct{}

	// in-flight request tracking
	reqMu   sync.Mutex
	nextReq atomic.Uint64
	pending map[uint64]chan *proto.Frame

	// background frame reader goroutine exit signal
	readDone chan struct{}

	// ── Instance-level push routing ──

	// pushMu guards pushRouters.
	pushMu sync.RWMutex
	// pushRouters maps router ID → handler function.
	// Using a map + ID avoids the reflect package.
	pushRouters  map[uint64]func(*proto.Frame) bool
	nextRouterID atomic.Uint64

	// ── pushActive prevents read-deadline from killing idle push conns ──
	// pushActive is incremented by Subscribe and decremented by Consumer.Close.
	pushActive atomic.Int32
}

// Dial opens a TCP connection to addr, performs a Ping/Pong handshake, and
// returns a ready Client. All subsequent operations use this connection.
// Respects WithDialTimeout (default 10 s).
func Dial(addr string, opts ...Option) (*Client, error) {
	o := defaultClientOptions()
	for _, fn := range opts {
		fn(&o)
	}

	var rawConn net.Conn
	var err error
	if o.tlsCfg != nil {
		rawConn, err = tls.DialWithDialer(&net.Dialer{Timeout: o.dialTimeout}, "tcp", addr, o.tlsCfg)
	} else {
		rawConn, err = net.DialTimeout("tcp", addr, o.dialTimeout)
	}
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}

	c := &Client{
		opts:        o,
		conn:        rawConn,
		enc:         proto.NewEncoder(bufio.NewWriterSize(rawConn, 64*1024)),
		dec:         proto.NewDecoder(rawConn),
		closeCh:     make(chan struct{}),
		pending:     make(map[uint64]chan *proto.Frame),
		readDone:    make(chan struct{}),
		pushRouters: make(map[uint64]func(*proto.Frame) bool),
	}

	// Ping/Pong handshake.
	if err := c.ping(); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("client: handshake: %w", err)
	}

	// Clear the deadline set during the handshake before readLoop
	// takes ownership of deadline management.
	_ = c.conn.SetReadDeadline(time.Time{})

	// Start the background reader that demultiplexes broker frames.
	go c.readLoop()

	return c, nil
}

// ping sends CmdPing and waits for CmdPong.
// It uses dialTimeout (not readTimeout) for the one-time handshake so that a
// short readTimeout does not race against the not-yet-started readLoop.
func (c *Client) ping() error {
	if c.opts.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.writeTimeout))
	}
	if err := c.enc.Encode(proto.CmdPing, 0, nil); err != nil {
		return fmt.Errorf("send ping: %w", err)
	}
	// C3: the encoder writes into a 64 KiB bufio.Writer; flush immediately so
	// the Ping reaches the broker before we block waiting for the Pong.
	if err := c.enc.Flush(); err != nil {
		return fmt.Errorf("flush ping: %w", err)
	}
	// Use dialTimeout for the handshake read, not readTimeout.
	if c.opts.dialTimeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.opts.dialTimeout))
	}
	f, err := c.dec.Decode()
	if err != nil {
		return fmt.Errorf("read pong: %w", err)
	}
	if f.Command != proto.CmdPong {
		return fmt.Errorf("expected PONG, got %s", f.Command)
	}
	return nil
}

// readLoop demultiplexes incoming frames and routes them to waiting callers
// or to the push-consumer receive loop.
// When at least one push consumer is active, the read deadline is
// cleared so idle connections are not killed by the timeout.
func (c *Client) readLoop() {
	defer close(c.readDone)
	for {
		// Manage deadline based on push-consumer activity.
		if c.pushActive.Load() > 0 {
			// At least one push consumer is active — clear the deadline so
			// the connection stays open between infrequent push messages.
			_ = c.conn.SetReadDeadline(time.Time{})
		} else if c.opts.readTimeout > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.opts.readTimeout))
		}

		f, err := c.dec.Decode()
		if err != nil {
			if c.closed.Load() {
				return
			}
			// Connection error: unblock all pending callers.
			c.failAll(&ErrConnectionClosed{Cause: err})
			return
		}

		switch f.Command {
		case proto.CmdPush:
			// Push frames are handled by the pushRouter registered by Consumer.Subscribe.
			c.routePush(f)
		default:
			c.route(f)
		}
	}
}

// route delivers a response frame to the waiting caller.
func (c *Client) route(f *proto.Frame) {
	c.reqMu.Lock()
	ch, ok := c.pending[f.RequestID]
	if ok {
		delete(c.pending, f.RequestID)
	}
	c.reqMu.Unlock()
	if ok {
		ch <- f
	}
}

// failAll unblocks all pending callers with a closed-connection sentinel.
func (c *Client) failAll(err error) {
	c.reqMu.Lock()
	pending := c.pending
	c.pending = make(map[uint64]chan *proto.Frame)
	c.reqMu.Unlock()
	// Deliver a synthetic error frame to each waiter.
	for _, ch := range pending {
		select {
		case ch <- nil: // nil signals the caller to return ErrConnectionClosed
		default:
		}
	}
	_ = err
}

// ─── Push routing ────────────────────────────────────────────────────────────

// registerPushRouter registers fn as a push-frame handler for this Client
// instance and returns a unique ID that can be passed to deregisterPushRouter.
// Uses instance-level map instead of a process-global slice.
func (c *Client) registerPushRouter(fn func(*proto.Frame) bool) uint64 {
	id := c.nextRouterID.Add(1)
	c.pushMu.Lock()
	c.pushRouters[id] = fn
	c.pushMu.Unlock()
	return id
}

// deregisterPushRouter removes the router with the given ID.
// Proper O(1) removal using the router ID.
func (c *Client) deregisterPushRouter(id uint64) {
	c.pushMu.Lock()
	delete(c.pushRouters, id)
	c.pushMu.Unlock()
}

// routePush delivers f to the first handler that claims it (returns true).
func (c *Client) routePush(f *proto.Frame) {
	c.pushMu.RLock()
	// Copy IDs so we can release the lock before calling handlers.
	ids := make([]uint64, 0, len(c.pushRouters))
	fns := make([]func(*proto.Frame) bool, 0, len(c.pushRouters))
	for id, fn := range c.pushRouters {
		ids = append(ids, id)
		fns = append(fns, fn)
	}
	c.pushMu.RUnlock()
	_ = ids
	for _, fn := range fns {
		if fn(f) {
			return
		}
	}
}

// ─── Request/response helpers ─────────────────────────────────────────────────

// sendRecv sends a command frame and blocks until the broker replies.
func (c *Client) sendRecv(ctx context.Context, cmd proto.Command, body interface{}) (*proto.Frame, error) {
	if c.closed.Load() {
		return nil, &ErrConnectionClosed{}
	}

	reqID := c.nextReq.Add(1)
	ch := make(chan *proto.Frame, 1)

	c.reqMu.Lock()
	c.pending[reqID] = ch
	c.reqMu.Unlock()

	// Write the request.
	c.mu.Lock()
	if c.opts.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.writeTimeout))
	}
	err := c.enc.Encode(cmd, reqID, body)
	if err == nil {
		// C3: flush the 64 KiB write buffer so the request is sent immediately.
		err = c.enc.Flush()
	}
	c.mu.Unlock()

	if err != nil {
		c.reqMu.Lock()
		delete(c.pending, reqID)
		c.reqMu.Unlock()
		return nil, &ErrConnectionClosed{Cause: err}
	}

	// Wait for the reply.
	select {
	case f := <-ch:
		if f == nil {
			return nil, &ErrConnectionClosed{}
		}
		if f.Command == proto.CmdError {
			var e proto.ErrorResponse
			_ = proto.Unmarshal(f, &e)
			return nil, mapBrokerError(e.Code, e.Message)
		}
		return f, nil
	case <-ctx.Done():
		c.reqMu.Lock()
		delete(c.pending, reqID)
		c.reqMu.Unlock()
		return nil, fmt.Errorf("client: request cancelled: %w", ctx.Err())
	case <-c.closeCh:
		return nil, &ErrConnectionClosed{}
	}
}

// Authenticate sends an API key to the broker and returns nil on success.
func (c *Client) Authenticate(apiKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.sendRecv(ctx, proto.CmdAuth, &proto.AuthRequest{APIKey: apiKey})
	if err != nil {
		return fmt.Errorf("authenticate: %w", err)
	}
	return nil
}

// Addr returns the remote broker address this Client is connected to, in
// the same form passed to Dial (or its resolved net.Conn.RemoteAddr() form
// if the connection has since been established). Useful for callers — such
// as the HTTP gateway — that need to open additional connections to the
// same broker (e.g. one per WebSocket subscriber).
func (c *Client) Addr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return ""
	}
	return c.conn.RemoteAddr().String()
}

// Close closes the underlying TCP connection and stops all goroutines.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.closeCh)
	err := c.conn.Close()
	// Wait for readLoop to exit.
	<-c.readDone
	return err
}

// isConnClosed reports whether the connection is closed.
func isConnClosed(err error) bool {
	return err != nil && (err == io.EOF || err == io.ErrClosedPipe || err == net.ErrClosed)
}

// PushRouterCount returns the number of currently registered push routers.
// This is exported for testing router cleanup; callers should not
// rely on this value in production code.
func (c *Client) PushRouterCount() int {
	c.pushMu.RLock()
	defer c.pushMu.RUnlock()
	return len(c.pushRouters)
}
