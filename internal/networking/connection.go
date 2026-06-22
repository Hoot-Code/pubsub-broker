package networking

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
)

// writeBufSize is the size of the per-connection write buffer.
// 64 KiB amortises the overhead of many small frame writes into fewer syscalls.
const writeBufSize = 64 * 1024

// authorizer is satisfied by *auth.Identity without importing the auth package
// (which would create an import cycle). It is the minimal interface required
// to enforce role-based access control and topic-level ACLs at the connection
// level.
type authorizer interface {
	// Can reports whether the identity is permitted to perform perm on topic.
	// perm is one of the PermXxx string constants from the auth package.
	// topic may be empty for permission checks that are not topic-specific.
	Can(perm, topic string) bool
}

// Conn wraps a net.Conn with protocol codecs, deadlines, auth state, and a
// 64 KiB write buffer so that small frames (ACKs, errors) are coalesced into
// fewer syscalls.
type Conn struct {
	mu           sync.Mutex
	raw          net.Conn
	bw           *bufio.Writer
	enc          *protocol.Encoder
	dec          *protocol.Decoder
	cfg          *config.NetworkConfig
	closed       atomic.Bool
	authed       atomic.Bool
	clientID     string
	perms        []string
	identity     authorizer // RBAC identity set after authentication
	remoteAddr   string
	connectedAt  time.Time
	bytesRead    atomic.Int64
	bytesWritten atomic.Int64
}

func newConn(raw net.Conn, cfg *config.NetworkConfig) *Conn {
	bw := bufio.NewWriterSize(raw, writeBufSize)
	return &Conn{
		raw:         raw,
		bw:          bw,
		enc:         protocol.NewEncoder(bw),
		dec:         protocol.NewDecoder(raw),
		cfg:         cfg,
		remoteAddr:  raw.RemoteAddr().String(),
		connectedAt: time.Now(),
	}
}

// ReadFrame reads the next frame, applying a read deadline.
func (c *Conn) ReadFrame() (*protocol.Frame, error) {
	if c.cfg.ReadTimeout > 0 {
		_ = c.raw.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
	}
	f, err := c.dec.Decode()
	if err != nil {
		return nil, err
	}
	c.bytesRead.Add(int64(protocol.HeaderSize + len(f.Body)))
	return f, nil
}

// WriteFrame encodes a frame into the write buffer and then flushes it.
// The flush ensures the frame is sent immediately for latency-sensitive paths.
func (c *Conn) WriteFrame(cmd protocol.Command, reqID uint64, body interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.WriteTimeout > 0 {
		_ = c.raw.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	}
	if err := c.enc.Encode(cmd, reqID, body); err != nil {
		return err
	}
	return c.bw.Flush()
}

// WriteFrameUnflushed encodes a frame into the write buffer WITHOUT flushing.
// The caller must call Flush() when the batch is complete.
func (c *Conn) WriteFrameUnflushed(cmd protocol.Command, reqID uint64, body interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.WriteTimeout > 0 {
		_ = c.raw.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	}
	return c.enc.Encode(cmd, reqID, body)
}

// Flush flushes any buffered write data to the underlying network connection.
// Call after a batch of WriteFrameUnflushed calls.
func (c *Conn) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bw.Flush()
}

// SendOK sends a simple success response.
func (c *Conn) SendOK(reqID uint64) error {
	return c.WriteFrame(protocol.CmdResponse, reqID, &protocol.OKResponse{OK: true})
}

// SendError sends an error response.
func (c *Conn) SendError(reqID uint64, code, msg string) error {
	return c.WriteFrame(protocol.CmdError, reqID, &protocol.ErrorResponse{
		Code:    code,
		Message: msg,
	})
}

// RemoteAddr returns the remote address string.
func (c *Conn) RemoteAddr() string { return c.remoteAddr }

// SetAuth marks the connection as authenticated with the given clientID and
// legacy permission list. For full RBAC, call SetIdentity in addition.
func (c *Conn) SetAuth(clientID string, perms []string) {
	c.mu.Lock()
	c.clientID = clientID
	c.perms = perms
	c.mu.Unlock()
	c.authed.Store(true)
}

// SetIdentity attaches an RBAC identity to this connection. The identity is
// used by Can() to enforce role-based and topic-level access control.
// a must be non-nil (pass a sentinel no-op identity rather than nil to revoke).
func (c *Conn) SetIdentity(a authorizer) {
	c.mu.Lock()
	c.identity = a
	c.mu.Unlock()
}

// Can reports whether this connection is permitted to perform perm on topic.
// If an RBAC identity has been set via SetIdentity, it delegates to that.
// Otherwise it falls back to the legacy perms slice from SetAuth.
// perm should be one of the auth.PermXxx string constants.
// topic may be empty for non-topic-specific checks.
func (c *Conn) Can(perm, topic string) bool {
	c.mu.Lock()
	id := c.identity
	perms := c.perms
	c.mu.Unlock()
	if id != nil {
		return id.Can(perm, topic)
	}
	// Legacy fallback: check perms slice from SetAuth.
	for _, p := range perms {
		if p == perm || p == "admin" {
			return true
		}
	}
	return false
}

// IsAuthed reports whether this connection is authenticated.
func (c *Conn) IsAuthed() bool { return c.authed.Load() }

// ClientID returns the authenticated client identifier.
func (c *Conn) ClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

// HasPerm reports whether the connection has the given legacy permission.
// Prefer Can(perm, topic) for RBAC-aware checks.
func (c *Conn) HasPerm(perm string) bool {
	return c.Can(perm, "")
}

// IsClosed reports whether the connection is closed.
func (c *Conn) IsClosed() bool { return c.closed.Load() }

func (c *Conn) close() {
	if c.closed.CompareAndSwap(false, true) {
		_ = c.raw.Close()
	}
}

// RawConn returns the underlying net.Conn.
// Used by storage.SendTo for zero-copy sendfile transfers.
func (c *Conn) RawConn() net.Conn { return c.raw }

// NewTestConn exposes newConn for use in external test packages.
// It should only be used in tests.
func NewTestConn(raw net.Conn, cfg *config.NetworkConfig) *Conn {
	return newConn(raw, cfg)
}
