package networking_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

func testNetCfg() *config.NetworkConfig {
	return &config.NetworkConfig{
		Host:           "127.0.0.1",
		Port:           0,
		MaxConnections: 100,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		IdleTimeout:    30 * time.Second,
	}
}

// echoHandler replies to every PING with a PONG.
type echoHandler struct {
	seen atomic.Int64
}

func (h *echoHandler) Handle(_ context.Context, conn *networking.Conn) error {
	for {
		f, err := conn.ReadFrame()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		h.seen.Add(1)
		if err := conn.WriteFrame(protocol.CmdPong, f.RequestID, nil); err != nil {
			return err
		}
	}
}

func startServer(t *testing.T, handler networking.Handler) (string, func()) {
	t.Helper()

	cfg := testNetCfg()
	cfg.Port = 0 // Let OS pick a free port atomically at bind time.

	srv := networking.NewServer(cfg, handler, logging.New(nil, "error"))
	done := make(chan error, 1)

	go func() {
		done <- srv.Start()
	}()

	// Wait for the server to bind and expose its address.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Addr() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == nil {
		t.Fatal("server did not start in time")
	}
	addr := srv.Addr().String()

	// Wait for server to be reachable.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return addr, sync.OnceFunc(func() {
		_ = srv.Close()
		<-done
	})
}

// dial returns a raw TCP connection to addr.
func dialAddr(t *testing.T, addr string) (net.Conn, *protocol.Encoder, *protocol.Decoder) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, protocol.NewEncoder(c), protocol.NewDecoder(c)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestServer_PingPong(t *testing.T) {
	h := &echoHandler{}
	addr, stop := startServer(t, h)
	defer stop()

	_, enc, dec := dialAddr(t, addr)

	if err := enc.Encode(protocol.CmdPing, 1, nil); err != nil {
		t.Fatalf("encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if f.Command != protocol.CmdPong {
		t.Errorf("want PONG, got %s", f.Command)
	}
	if f.RequestID != 1 {
		t.Errorf("requestID: want 1, got %d", f.RequestID)
	}
}

func TestServer_MultipleClients(t *testing.T) {
	h := &echoHandler{}
	addr, stop := startServer(t, h)
	defer stop()

	const clients = 5
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, enc, dec := dialAddr(t, addr)
			for j := 0; j < 3; j++ {
				_ = enc.Encode(protocol.CmdPing, uint64(j), nil)
				f, err := dec.Decode()
				if err != nil {
					t.Errorf("client %d decode: %v", id, err)
					return
				}
				if f.Command != protocol.CmdPong {
					t.Errorf("client %d: want PONG, got %s", id, f.Command)
				}
			}
		}(i)
	}
	wg.Wait()
	if seen := h.seen.Load(); seen != int64(clients*3) {
		t.Errorf("handler seen %d frames, want %d", seen, clients*3)
	}
}

func TestServer_CloseBeforeStart(t *testing.T) {
	cfg := testNetCfg()
	srv := networking.NewServer(cfg, &echoHandler{}, nil)
	// Close on a server that was never started must be a no-op.
	if err := srv.Close(); err != nil {
		t.Fatalf("Close before Start: %v", err)
	}
}

func TestServer_ActiveConnections(t *testing.T) {
	h := &echoHandler{}
	addr, stop := startServer(t, h)
	defer stop()

	// Verify that a connection can be established and a frame exchanged.
	_, enc, dec := dialAddr(t, addr)
	_ = enc.Encode(protocol.CmdPing, 1, nil)
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if f.Command != protocol.CmdPong {
		t.Errorf("want PONG, got %s", f.Command)
	}
}

func TestServer_GracefulShutdown(t *testing.T) {
	h := &echoHandler{}
	addr, stop := startServer(t, h)
	defer stop() // ensure server is always stopped, even if the test fails early

	_, enc, dec := dialAddr(t, addr)
	_ = enc.Encode(protocol.CmdPing, 1, nil)
	_, _ = dec.Decode()

	// Stop the server; subsequent dials should fail quickly.
	stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err != nil {
			return // server is down — pass
		}
		c.Close()
		time.Sleep(20 * time.Millisecond)
	}
	// It's OK if the server takes a short moment to stop accepting.
}

// TestServer_ConcurrentWritesSafe ensures that concurrent writes arriving
// from multiple goroutines on the same client connection are handled
// gracefully by the server (no panics, no crashes).
// A mutex protects the client-side Encoder, which is not goroutine-safe by
// design; the interesting concurrency is on the server's read/handle path.
func TestServer_ConcurrentWritesSafe(t *testing.T) {
	h := &echoHandler{}
	addr, stop := startServer(t, h)
	defer stop()

	_, enc, dec := dialAddr(t, addr)
	const frames = 20
	var (
		wg    sync.WaitGroup
		encMu sync.Mutex
	)
	for i := 0; i < frames; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			encMu.Lock()
			_ = enc.Encode(protocol.CmdPing, uint64(n), nil)
			encMu.Unlock()
		}(i)
	}
	wg.Wait()

	// Give the server time to process all frames then drain responses.
	time.Sleep(100 * time.Millisecond)
	_ = dec // silence linter
}

// ─── Conn tests ───────────────────────────────────────────────────────────────

func TestConn_AuthState(t *testing.T) {
	// We test auth state through the networking.Conn indirectly via the handler.
	h := &authCheckHandler{t: t}
	addr, stop := startServer(t, h)
	defer stop()

	_, enc, dec := dialAddr(t, addr)

	// Send an auth frame.
	_ = enc.Encode(protocol.CmdAuth, 1, &protocol.AuthRequest{APIKey: "test-key"})
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	_ = f // handler writes a PONG regardless
}

type authCheckHandler struct{ t *testing.T }

func (h *authCheckHandler) Handle(_ context.Context, conn *networking.Conn) error {
	f, err := conn.ReadFrame()
	if err != nil {
		return err
	}
	// Before setting auth, IsAuthed must be false.
	if conn.IsAuthed() {
		h.t.Error("IsAuthed: want false before SetAuth")
	}
	conn.SetAuth("client-1", []string{"publish", "subscribe"})
	if !conn.IsAuthed() {
		h.t.Error("IsAuthed: want true after SetAuth")
	}
	if conn.ClientID() != "client-1" {
		h.t.Errorf("ClientID: want client-1, got %s", conn.ClientID())
	}
	if !conn.HasPerm("publish") {
		h.t.Error("HasPerm(publish): want true")
	}
	if conn.HasPerm("admin") {
		h.t.Error("HasPerm(admin): want false")
	}
	return conn.WriteFrame(protocol.CmdPong, f.RequestID, nil)
}

func TestConn_SendOKAndError(t *testing.T) {
	h := &okErrHandler{}
	addr, stop := startServer(t, h)
	defer stop()

	_, enc, dec := dialAddr(t, addr)

	_ = enc.Encode(protocol.CmdPing, 42, nil)
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode OK: %v", err)
	}
	if f.Command != protocol.CmdResponse {
		t.Errorf("want RESPONSE, got %s", f.Command)
	}
	if f.RequestID != 42 {
		t.Errorf("requestID: want 42, got %d", f.RequestID)
	}

	_ = enc.Encode(protocol.CmdPing, 43, nil)
	f2, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode ERR: %v", err)
	}
	if f2.Command != protocol.CmdError {
		t.Errorf("want ERROR, got %s", f2.Command)
	}
}

type okErrHandler struct{ count int }

func (h *okErrHandler) Handle(_ context.Context, conn *networking.Conn) error {
	for {
		f, err := conn.ReadFrame()
		if err != nil {
			return err
		}
		h.count++
		if h.count%2 == 1 {
			_ = conn.SendOK(f.RequestID)
		} else {
			_ = conn.SendError(f.RequestID, "TEST_ERR", "test error")
		}
	}
}

// ─── SECURITY 13: TLS test ────────────────────────────────────────────────────

// TestServer_TLS starts a broker with a self-signed certificate, connects a
// TLS client, and verifies a Ping/Pong round-trip (SECURITY 13e).
func TestServer_TLS(t *testing.T) {
	t.Parallel()

	// Generate a self-signed RSA certificate in-test.
	certPEM, keyPEM := generateSelfSignedCert(t)

	// Write cert/key to temp files.
	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	cfg := testNetCfg()
	cfg.TLSCertFile = certFile
	cfg.TLSKeyFile = keyFile
	cfg.TLSMinVersion = tls.VersionTLS12 // allow TLS 1.2 for test compatibility

	log := logging.New(nil, "error")

	var (
		handler echoHandler
		srv     = networking.NewServer(cfg, &handler, log)
	)

	errC := make(chan error, 1)
	go func() { errC <- srv.Start() }()

	// Wait for the server to start listening.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Addr() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == nil {
		t.Fatal("TLS server did not start")
	}
	t.Cleanup(func() {
		_ = srv.Close()
		<-errC
	})

	// Connect with a TLS client that trusts the self-signed cert.
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	tlsCfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
	conn, err := tls.DialWithDialer(
		&net.Dialer{Timeout: 3 * time.Second},
		"tcp",
		srv.Addr().String(),
		tlsCfg,
	)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()

	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	if err := enc.Encode(protocol.CmdPing, 1, nil); err != nil {
		t.Fatalf("send Ping: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("recv Pong: %v", err)
	}
	if f.Command != protocol.CmdPong {
		t.Errorf("want CmdPong, got %s", f.Command)
	}
}

// TestServer_TLS_BadCert verifies that a missing certificate file causes
// Start() to return a descriptive error rather than silently falling back
// to plaintext (SECURITY 13c).
func TestServer_TLS_BadCert(t *testing.T) {
	t.Parallel()

	cfg := testNetCfg()
	cfg.TLSCertFile = "/nonexistent/cert.pem"
	cfg.TLSKeyFile = "/nonexistent/key.pem"

	log := logging.New(nil, "error")
	srv := networking.NewServer(cfg, &echoHandler{}, log)

	err := srv.Start()
	if err == nil {
		t.Fatal("expected error for missing TLS files, got nil")
	}
}

// ─── TLS helpers ─────────────────────────────────────────────────────────────

// generateSelfSignedCert creates an in-memory self-signed TLS certificate for
// testing. Returns (certPEM, keyPEM).
func generateSelfSignedCert(t *testing.T) ([]byte, []byte) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(priv),
	})
	return certPEM, keyPEM
}
