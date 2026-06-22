package broker

import (
	"context"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/auth"
)

func TestSessionCreateAndGet(t *testing.T) {
	store := NewSessionStore(time.Hour)
	id := &auth.Identity{ClientID: "test-user", Role: auth.RoleAdmin}
	sess, err := store.Create(id, "127.0.0.1:12345")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if sess.Token == "" {
		t.Fatal("session token is empty")
	}
	if sess.Identity.ClientID != "test-user" {
		t.Errorf("ClientID = %q, want %q", sess.Identity.ClientID, "test-user")
	}
	if sess.ExpiresAt.Before(time.Now()) {
		t.Error("session already expired")
	}

	got, ok := store.Get(sess.Token)
	if !ok {
		t.Fatal("Get returned false for valid token")
	}
	if got.Token != sess.Token {
		t.Errorf("Get token mismatch: %q != %q", got.Token, sess.Token)
	}
}

func TestSessionExpiry(t *testing.T) {
	store := NewSessionStore(50 * time.Millisecond)
	id := &auth.Identity{ClientID: "short-lived", Role: auth.RoleViewer}
	sess, err := store.Create(id, "127.0.0.1:9999")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Immediately should work.
	if _, ok := store.Get(sess.Token); !ok {
		t.Fatal("Get failed immediately after Create")
	}

	time.Sleep(60 * time.Millisecond)

	if _, ok := store.Get(sess.Token); ok {
		t.Fatal("Get succeeded after expiry")
	}
}

func TestSessionRevoke(t *testing.T) {
	store := NewSessionStore(time.Hour)
	id := &auth.Identity{ClientID: "revoked-user", Role: auth.RoleProducer}
	sess, err := store.Create(id, "127.0.0.1:5555")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	store.Revoke(sess.Token)

	if _, ok := store.Get(sess.Token); ok {
		t.Fatal("Get succeeded after Revoke")
	}
}

func TestSessionCleanupEvictsExpired(t *testing.T) {
	store := NewSessionStore(30 * time.Millisecond)
	id := &auth.Identity{ClientID: "cleanup-user", Role: auth.RoleConsumer}
	sess, err := store.Create(id, "127.0.0.1:7777")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.StartCleanup(ctx, 20*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	if _, ok := store.Get(sess.Token); ok {
		t.Fatal("expired session was not evicted by cleanup")
	}
}

func TestSessionStoreDefaultTTL(t *testing.T) {
	store := NewSessionStore(0) // zero → default
	id := &auth.Identity{ClientID: "default-ttl", Role: auth.RoleAdmin}
	sess, err := store.Create(id, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	expected := time.Now().Add(12 * time.Hour)
	if sess.ExpiresAt.Sub(expected) > time.Second {
		t.Errorf("default TTL not 12h: ExpiresAt=%v", sess.ExpiresAt)
	}
}

func TestSessionGetNonexistent(t *testing.T) {
	store := NewSessionStore(time.Hour)
	if _, ok := store.Get("does-not-exist"); ok {
		t.Fatal("Get returned true for nonexistent token")
	}
}
