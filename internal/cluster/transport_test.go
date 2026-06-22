package cluster

import (
	"testing"
	"time"
)

// TestTransportSendRecv starts two Transports on :0, sends 10 messages from A
// to B, and verifies all 10 arrive on B.Recv() in order.
func TestTransportSendRecv(t *testing.T) {
	a, err := NewTransport(":0")
	if err != nil {
		t.Fatalf("NewTransport A: %v", err)
	}
	defer a.Close()

	b, err := NewTransport(":0")
	if err != nil {
		t.Fatalf("NewTransport B: %v", err)
	}
	defer b.Close()

	const n = 10
	for i := uint64(0); i < n; i++ {
		msg := &ClusterMsg{
			Type: MsgHeartbeat,
			From: "node-a",
			Term: i + 1,
		}
		if err := a.Send(b.Addr(), msg); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
	}

	recv := b.Recv()
	for i := uint64(0); i < n; i++ {
		select {
		case msg := <-recv:
			if msg.Term != i+1 {
				t.Errorf("msg[%d]: Term = %d, want %d", i, msg.Term, i+1)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

// TestTransportClose verifies that Close shuts down gracefully.
func TestTransportClose(t *testing.T) {
	tr, err := NewTransport(":0")
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Double-close must not panic.
	_ = tr.Close()
}

// TestTransportMTLS verifies that two mTLS-configured transports can exchange
// messages, and that a third transport without mTLS is rejected.
func TestTransportMTLS(t *testing.T) {
	certPEM, keyPEM, caPEM := generateTestCerts(t)

	certFile := writeTempPEM(t, "cert.pem", certPEM)
	keyFile := writeTempPEM(t, "key.pem", keyPEM)
	caFile := writeTempPEM(t, "ca.pem", caPEM)

	cfg := TransportConfig{
		BindAddr:     "127.0.0.1:0",
		MTLSCertFile: certFile,
		MTLSKeyFile:  keyFile,
		MTLSCAFile:   caFile,
	}

	// Start two mTLS-enabled transports.
	a, err := NewTransportWithConfig(cfg)
	if err != nil {
		t.Fatalf("NewTransportWithConfig A: %v", err)
	}
	defer a.Close()

	b, err := NewTransportWithConfig(cfg)
	if err != nil {
		t.Fatalf("NewTransportWithConfig B: %v", err)
	}
	defer b.Close()

	// Send 10 messages from A → B over mTLS.
	const n = 10
	for i := uint64(0); i < n; i++ {
		msg := &ClusterMsg{Type: MsgHeartbeat, From: "node-a", Term: i + 1}
		if err := a.Send(b.Addr(), msg); err != nil {
			t.Fatalf("mTLS Send[%d]: %v", i, err)
		}
	}

	recv := b.Recv()
	for i := uint64(0); i < n; i++ {
		select {
		case msg := <-recv:
			if msg.Term != i+1 {
				t.Errorf("msg[%d]: Term=%d want %d", i, msg.Term, i+1)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for mTLS message %d", i)
		}
	}

	// A plain-TCP transport must fail to connect to an mTLS listener.
	plain, err := NewTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewTransport plain: %v", err)
	}
	defer plain.Close()

	badMsg := &ClusterMsg{Type: MsgHeartbeat, From: "intruder", Term: 999}
	if err := plain.Send(b.Addr(), badMsg); err == nil {
		// Allow for the handshake error arriving asynchronously: verify nothing
		// was delivered to b within a short window.
		select {
		case got := <-b.Recv():
			if got.Term == 999 {
				t.Error("plain-TCP intruder message was accepted by mTLS listener")
			}
		case <-time.After(300 * time.Millisecond):
			// Good: no message arrived.
		}
	}
	// Either Send returned an error (immediate TLS rejection) or the message
	// was dropped; both are acceptable.
}
