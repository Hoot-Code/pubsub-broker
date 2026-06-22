package auth_test

import (
	"fmt"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

// BenchmarkAuthenticate measures Authenticate() performance with a valid key.
func BenchmarkAuthenticate(b *testing.B) {
	const rawKey = "super-secret-bench-key-12345"
	a := auth.NewAuthenticator(config.AuthConfig{
		Enabled: true,
		APIKeys: []config.APIKeyEntry{
			{Key: rawKey, ClientID: "bench-client", Role: "producer"},
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, err := a.Authenticate(rawKey)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		if id == nil {
			b.Fatal("nil identity")
		}
	}
}

// BenchmarkAuthenticateInvalid measures Authenticate() with an invalid key.
func BenchmarkAuthenticateInvalid(b *testing.B) {
	a := auth.NewAuthenticator(config.AuthConfig{
		Enabled: true,
		APIKeys: []config.APIKeyEntry{
			{Key: "valid-key", ClientID: "c1", Role: "admin"},
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = a.Authenticate("not-valid")
	}
}

// TestAuthenticateSameErrorMessage verifies that two different invalid API keys
// produce exactly the same error string (no key prefix or detail exposed).
func TestAuthenticateSameErrorMessage(t *testing.T) {
	a := auth.NewAuthenticator(config.AuthConfig{
		Enabled: true,
		APIKeys: []config.APIKeyEntry{
			{Key: "registered-key", ClientID: "c1", Role: "admin"},
		},
	})
	_, err1 := a.Authenticate("bad-key-AAAA")
	_, err2 := a.Authenticate("bad-key-ZZZZ")
	if err1 == nil || err2 == nil {
		t.Fatal("expected errors for invalid keys")
	}
	if err1.Error() != err2.Error() {
		t.Errorf("error messages differ:\n  key1: %q\n  key2: %q", err1.Error(), err2.Error())
	}
	for _, key := range []string{"bad-key-AAAA", "bad-key-ZZZZ", "AAAA", "ZZZZ"} {
		if containsStr(err1.Error(), key) {
			t.Errorf("error message leaks key prefix %q: %q", key, err1.Error())
		}
	}
}

func containsStr(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && (s == sub ||
		(len(s) > len(sub) && (s[:len(sub)] == sub || containsStr(s[1:], sub))))
}

// Silence unused import.
var _ = fmt.Sprintf
