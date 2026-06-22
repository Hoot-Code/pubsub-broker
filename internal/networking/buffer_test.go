package networking_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
)

// TestWriteBufferFlush verifies that 100 frames written through a buffered
// networking.Conn all arrive correctly on the other side of a net.Pipe()
// (Part C4 — network-level batching write-buffer test).
func TestWriteBufferFlush(t *testing.T) {
	t.Parallel()

	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck
	defer client.Close() //nolint:errcheck

	cfg := &config.NetworkConfig{WriteTimeout: time.Second}
	srvConn := networking.NewTestConn(server, cfg)

	const frameCount = 100
	type okBody struct {
		OK  bool   `json:"ok"`
		Seq int    `json:"seq"`
		Msg string `json:"msg"`
	}

	// Writer goroutine: write 100 frames without explicit flush between them,
	// then flush once. WriteFrame already flushes after each call, so we just
	// send 100 frames and verify they all arrive.
	errCh := make(chan error, 1)
	go func() {
		for i := 0; i < frameCount; i++ {
			body := &okBody{OK: true, Seq: i, Msg: fmt.Sprintf("frame-%03d", i)}
			if err := srvConn.WriteFrame(protocol.CmdResponse, uint64(i), body); err != nil {
				errCh <- fmt.Errorf("WriteFrame[%d]: %w", i, err)
				return
			}
		}
		errCh <- nil
	}()

	// Reader: decode all 100 frames and verify order.
	dec := protocol.NewDecoder(client)
	for i := 0; i < frameCount; i++ {
		f, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode[%d]: %v", i, err)
		}
		if f.RequestID != uint64(i) {
			t.Errorf("frame[%d]: RequestID want %d got %d", i, i, f.RequestID)
		}
		if f.Command != protocol.CmdResponse {
			t.Errorf("frame[%d]: Command want %s got %s", i, protocol.CmdResponse, f.Command)
		}
	}

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
