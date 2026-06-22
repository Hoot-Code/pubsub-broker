package config_test

import (
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

func TestApplyPatchValidField(t *testing.T) {
	base := config.Config{}
	base.RateLimit.PerClientRPS = 1000

	patched, err := config.ApplyPatch(base, map[string]interface{}{
		"rate_limit.per_client_rps": float64(50),
	})
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if patched.RateLimit.PerClientRPS != 50 {
		t.Errorf("per_client_rps: want 50, got %d", patched.RateLimit.PerClientRPS)
	}
	// Verify base is untouched (value copy semantics).
	if base.RateLimit.PerClientRPS != 1000 {
		t.Errorf("base was mutated: per_client_rps = %d, want 1000", base.RateLimit.PerClientRPS)
	}
}

func TestApplyPatchRejectsNonWhitelistedField(t *testing.T) {
	base := config.Config{}
	base.Network.Port = 9000

	_, err := config.ApplyPatch(base, map[string]interface{}{
		"network.port": float64(8080),
	})
	if err == nil {
		t.Fatal("expected error for non-hot-reloadable field")
	}
	if !contains(err.Error(), "requires a restart") {
		t.Errorf("error should mention restart: %v", err)
	}
	// Config should be zero (invalid).
}

func TestApplyPatchRejectsInvalidValue(t *testing.T) {
	base := config.Config{}
	base.RateLimit.PerClientRPS = 100

	_, err := config.ApplyPatch(base, map[string]interface{}{
		"rate_limit.per_client_rps": float64(-5),
	})
	if err == nil {
		t.Fatal("expected error for negative per_client_rps")
	}
}

func TestApplyPatchMultipleFields(t *testing.T) {
	base := config.Config{}
	base.RateLimit.PerClientRPS = 100
	base.Retention.MaxAgeHours = 24
	base.Logging.Level = "info"

	patched, err := config.ApplyPatch(base, map[string]interface{}{
		"rate_limit.per_client_rps": float64(200),
		"retention.max_age_hours":   float64(48),
		"logging.level":             "debug",
	})
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if patched.RateLimit.PerClientRPS != 200 {
		t.Errorf("per_client_rps: want 200, got %d", patched.RateLimit.PerClientRPS)
	}
	if patched.Retention.MaxAgeHours != 48 {
		t.Errorf("max_age_hours: want 48, got %d", patched.Retention.MaxAgeHours)
	}
	if patched.Logging.Level != "debug" {
		t.Errorf("logging.level: want debug, got %s", patched.Logging.Level)
	}
	// Base untouched.
	if base.RateLimit.PerClientRPS != 100 {
		t.Errorf("base mutated")
	}
}

func TestApplyPatchRejectsInvalidLogLevel(t *testing.T) {
	_, err := config.ApplyPatch(config.Config{}, map[string]interface{}{
		"logging.level": "verbose",
	})
	if err == nil {
		t.Fatal("expected error for invalid log level")
	}
}

func TestApplyPatchRetentionPolicyZeroDisabled(t *testing.T) {
	base := config.Config{}
	base.Retention.MaxAgeHours = 24

	patched, err := config.ApplyPatch(base, map[string]interface{}{
		"retention.max_age_hours": float64(0),
	})
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if patched.Retention.MaxAgeHours != 0 {
		t.Errorf("max_age_hours: want 0 (disabled), got %d", patched.Retention.MaxAgeHours)
	}
}

func TestIsHotReloadable(t *testing.T) {
	if !config.IsHotReloadable("rate_limit.per_client_rps") {
		t.Error("rate_limit.per_client_rps should be hot-reloadable")
	}
	if config.IsHotReloadable("network.port") {
		t.Error("network.port should NOT be hot-reloadable")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
