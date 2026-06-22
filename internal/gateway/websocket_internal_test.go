package gateway

import "testing"

// TestWebSocketHandshake verifies the Sec-WebSocket-Accept computation
// against the known RFC 6455 §1.3 test vector:
//
//	Sec-WebSocket-Key:    dGhlIHNhbXBsZSBub25jZQ==
//	Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
//
// This test delegates to the shared wsutil implementation.
func TestWebSocketHandshake(t *testing.T) {
	got := wsutilComputeAcceptKey("dGhlIHNhbXBsZSBub25jZQ==")
	want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	if got != want {
		t.Fatalf("Sec-WebSocket-Accept: want %q, got %q", want, got)
	}
}
