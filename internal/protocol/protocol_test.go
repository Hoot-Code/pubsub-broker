package protocol_test

import (
	"bytes"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
)

func roundTrip(t *testing.T, cmd protocol.Command, reqID uint64, body interface{}) *protocol.Frame {
	t.Helper()
	var buf bytes.Buffer
	enc := protocol.NewEncoder(&buf)
	if err := enc.Encode(cmd, reqID, body); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dec := protocol.NewDecoder(&buf)
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return f
}

func TestCodec_RoundTrip(t *testing.T) {
	req := &protocol.PublishRequest{
		Topic:   "orders",
		Key:     "customer-1",
		Payload: []byte(`{"amount":99.9}`),
	}
	f := roundTrip(t, protocol.CmdPublish, 42, req)

	if f.Command != protocol.CmdPublish {
		t.Errorf("command: want %s, got %s", protocol.CmdPublish, f.Command)
	}
	if f.RequestID != 42 {
		t.Errorf("requestID: want 42, got %d", f.RequestID)
	}

	var decoded protocol.PublishRequest
	if err := protocol.Unmarshal(f, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Topic != req.Topic {
		t.Errorf("topic: want %s, got %s", req.Topic, decoded.Topic)
	}
}

func TestCodec_EmptyBody(t *testing.T) {
	f := roundTrip(t, protocol.CmdPing, 1, nil)
	if f.Command != protocol.CmdPing {
		t.Errorf("command: want PING, got %s", f.Command)
	}
	if len(f.Body) != 0 {
		t.Errorf("expected empty body, got %d bytes", len(f.Body))
	}
}

func TestCodec_InvalidMagic(t *testing.T) {
	buf := bytes.Repeat([]byte{0xFF}, 18+4) // garbage
	dec := protocol.NewDecoder(bytes.NewReader(buf))
	_, err := dec.Decode()
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestCodec_MultipleFrames(t *testing.T) {
	var buf bytes.Buffer
	enc := protocol.NewEncoder(&buf)
	for i := uint64(0); i < 10; i++ {
		body := &protocol.OKResponse{OK: true}
		if err := enc.Encode(protocol.CmdResponse, i, body); err != nil {
			t.Fatalf("Encode[%d]: %v", i, err)
		}
	}
	dec := protocol.NewDecoder(&buf)
	for i := uint64(0); i < 10; i++ {
		f, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode[%d]: %v", i, err)
		}
		if f.RequestID != i {
			t.Errorf("frame[%d] requestID: want %d, got %d", i, i, f.RequestID)
		}
	}
}

func TestCodec_AllCommands(t *testing.T) {
	cmds := []protocol.Command{
		protocol.CmdPublish, protocol.CmdSubscribe, protocol.CmdUnsubscribe,
		protocol.CmdFetch, protocol.CmdAck, protocol.CmdNack,
		protocol.CmdCommitOffset, protocol.CmdCreateTopic, protocol.CmdDeleteTopic,
		protocol.CmdListTopics, protocol.CmdAuth, protocol.CmdPing,
		protocol.CmdPong, protocol.CmdResponse, protocol.CmdError,
	}
	for _, cmd := range cmds {
		f := roundTrip(t, cmd, 1, nil)
		if f.Command != cmd {
			t.Errorf("%s: round-trip command mismatch", cmd)
		}
	}
}

func BenchmarkCodec_EncodePublish(b *testing.B) {
	var buf bytes.Buffer
	enc := protocol.NewEncoder(&buf)
	req := &protocol.PublishRequest{
		Topic:   "bench",
		Payload: make([]byte, 256),
	}
	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = enc.Encode(protocol.CmdPublish, uint64(i), req)
	}
}
