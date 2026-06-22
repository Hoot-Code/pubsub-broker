package auth_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

func newAuth(enabled bool, keys ...config.APIKeyEntry) *auth.Authenticator {
	return auth.NewAuthenticator(config.AuthConfig{
		Enabled: enabled,
		APIKeys: keys,
	})
}

// TestAuthenticator_Disabled verifies that when auth is disabled, any key is
// accepted and granted RoleAdmin (all permissions).
func TestAuthenticator_Disabled(t *testing.T) {
	a := newAuth(false)
	id, err := a.Authenticate("any-key")
	if err != nil {
		t.Fatalf("disabled auth should allow any key: %v", err)
	}
	if !id.Can(string(auth.PermAdmin), "") {
		t.Error("disabled auth should grant admin")
	}
}

// TestAuthenticator_ValidKey verifies that a configured key returns the correct
// identity and role-derived permissions.
func TestAuthenticator_ValidKey(t *testing.T) {
	// Permissions: ["publish"] → roleFromLegacy → RoleProducer
	a := newAuth(true,
		config.APIKeyEntry{Key: "secret", ClientID: "svc-1", Permissions: []string{"publish"}},
	)
	id, err := a.Authenticate("secret")
	if err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	if id.ClientID != "svc-1" {
		t.Errorf("clientID: want svc-1, got %s", id.ClientID)
	}
	if !id.Can(string(auth.PermPublish), "") {
		t.Error("should have publish permission")
	}
	if id.Can(string(auth.PermSubscribe), "") {
		t.Error("should NOT have subscribe permission (RoleProducer)")
	}
}

func TestAuthenticator_InvalidKey(t *testing.T) {
	a := newAuth(true,
		config.APIKeyEntry{Key: "good", ClientID: "c", Permissions: []string{"publish"}},
	)
	_, err := a.Authenticate("bad-key")
	if err == nil {
		t.Fatal("expected error for invalid key")
	}
}

func TestAuthenticator_EmptyKey(t *testing.T) {
	a := newAuth(true)
	_, err := a.Authenticate("")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAuthenticator_AdminImpliesAll verifies that RoleAdmin grants every permission.
func TestAuthenticator_AdminImpliesAll(t *testing.T) {
	a := newAuth(true,
		config.APIKeyEntry{Key: "root", ClientID: "admin", Role: "admin"},
	)
	id, _ := a.Authenticate("root")
	for _, perm := range []string{
		string(auth.PermPublish), string(auth.PermSubscribe), string(auth.PermAdmin),
	} {
		if !id.Can(perm, "") {
			t.Errorf("admin should have %s permission", perm)
		}
	}
}

// TestAuthenticator_AddRevoke verifies AddKey and RemoveKey/RevokeKey.
func TestAuthenticator_AddRevoke(t *testing.T) {
	a := newAuth(true)
	a.AddKey("new-key", "new-client", auth.RoleConsumer, nil)
	id, err := a.Authenticate("new-key")
	if err != nil {
		t.Fatalf("added key rejected: %v", err)
	}
	if id.ClientID != "new-client" {
		t.Errorf("clientID: want new-client, got %s", id.ClientID)
	}
	a.RevokeKey("new-key")
	_, err = a.Authenticate("new-key")
	if err == nil {
		t.Fatal("revoked key should be rejected")
	}
}

func TestRateLimiter_Allow(t *testing.T) {
	rl := auth.NewRateLimiter(&config.RateLimitConfig{
		Enabled:         true,
		PerClientRPS:    5,
		PerTopicRPS:     100,
		BurstMultiplier: 1,
	})

	for i := 0; i < 5; i++ {
		if !rl.Allow("client-1", "topic-a") {
			t.Errorf("request %d should be allowed", i)
		}
	}
	if rl.Allow("client-1", "topic-a") {
		t.Error("6th request should be rate-limited")
	}
}

func TestRateLimiter_Disabled(t *testing.T) {
	rl := auth.NewRateLimiter(&config.RateLimitConfig{Enabled: false})
	for i := 0; i < 10000; i++ {
		if !rl.Allow("any", "any") {
			t.Fatal("disabled rate limiter should always allow")
		}
	}
}

func TestRateLimiter_Cleanup(t *testing.T) {
	t.Parallel()
	rl := auth.NewRateLimiter(&config.RateLimitConfig{
		Enabled: true, PerClientRPS: 1000, PerTopicRPS: 1000, BurstMultiplier: 1,
	})
	const n = 1000
	for i := 0; i < n; i++ {
		rl.Allow(fmt.Sprintf("client-%d", i), "topic-a")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rl.StartCleanup(ctx, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if !rl.Allow("brand-new-client", "brand-new-topic") {
		t.Error("Allow should succeed after cleanup evicted old buckets")
	}
}

func TestRateLimiter_CleanupStopped(t *testing.T) {
	t.Parallel()
	rl := auth.NewRateLimiter(&config.RateLimitConfig{
		Enabled: true, PerClientRPS: 10, PerTopicRPS: 10, BurstMultiplier: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	rl.StartCleanup(ctx, 100*time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
}
