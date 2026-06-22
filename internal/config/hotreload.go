// Package config — hotreload.go defines the whitelist of config fields that
// may be safely changed on a running broker without a restart, and provides
// the ApplyPatch function that produces a new Config with those fields changed
// while leaving the base Config untouched.
package config

import (
	"fmt"
	"strings"
	"time"
)

// HotReloadableFields is the authoritative set of config field paths that
// may be changed on a running broker without requiring a restart. Fields not
// in this set (network, TLS, cluster, storage, auth, etc.) are intentionally
// excluded because changing them at runtime would be unsafe or meaningless.
var HotReloadableFields = map[string]bool{
	"rate_limit.enabled":              true,
	"rate_limit.per_client_rps":       true,
	"rate_limit.per_topic_rps":        true,
	"retention.default_max_age_hours": false, // alias; actual path is retention.max_age_hours
	"retention.max_age_hours":         true,
	"retention.max_size_mb":           true,
	"compaction.interval_ms":          true,
	"compaction.tombstone_grace_ms":   true,
	"broker.drain_timeout_ms":         true,
	"drain_timeout_ms":                true,
	"broker.flow_control_pause_ms":    false, // alias; actual path is flow_control_pause_ms
	"flow_control_pause_ms":           true,
	"logging.level":                   true,
}

// IsHotReloadable reports whether the given dotted field path is in the
// hot-reload whitelist.
func IsHotReloadable(fieldPath string) bool {
	return HotReloadableFields[fieldPath]
}

// ApplyPatch takes a base Config and a map of dotted-field-path → new value
// (containing only keys already confirmed hot-reloadable by the caller) and
// returns a NEW Config with those fields changed. The base Config is never
// mutated. Per-field type validation happens here; an error is returned if
// any value is invalid, leaving the caller with a zero Config it must NOT use.
func ApplyPatch(base Config, patch map[string]interface{}) (Config, error) {
	cfg := base // value copy
	for fieldPath, raw := range patch {
		if err := applyField(&cfg, fieldPath, raw); err != nil {
			return Config{}, fmt.Errorf("apply patch %q: %w", fieldPath, err)
		}
	}
	return cfg, nil
}

// applyField sets a single dotted field on cfg after validating its type.
func applyField(cfg *Config, fieldPath string, raw interface{}) error {
	switch fieldPath {
	case "rate_limit.enabled":
		v, ok := raw.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", raw)
		}
		cfg.RateLimit.Enabled = v

	case "rate_limit.per_client_rps":
		v, err := toInt(raw)
		if err != nil {
			return fmt.Errorf("per_client_rps: %w", err)
		}
		if v <= 0 {
			return fmt.Errorf("per_client_rps must be > 0, got %d", v)
		}
		cfg.RateLimit.PerClientRPS = v

	case "rate_limit.per_topic_rps":
		v, err := toInt(raw)
		if err != nil {
			return fmt.Errorf("per_topic_rps: %w", err)
		}
		if v <= 0 {
			return fmt.Errorf("per_topic_rps must be > 0, got %d", v)
		}
		cfg.RateLimit.PerTopicRPS = v

	case "retention.max_age_hours":
		v, err := toInt(raw)
		if err != nil {
			return fmt.Errorf("max_age_hours: %w", err)
		}
		if v < 0 {
			return fmt.Errorf("max_age_hours must be >= 0, got %d", v)
		}
		cfg.Retention.MaxAgeHours = v

	case "retention.max_size_mb":
		v, err := toInt64(raw)
		if err != nil {
			return fmt.Errorf("max_size_mb: %w", err)
		}
		if v < 0 {
			return fmt.Errorf("max_size_mb must be >= 0, got %d", v)
		}
		cfg.Retention.MaxSizeMB = v

	case "compaction.interval_ms":
		v, err := toInt(raw)
		if err != nil {
			return fmt.Errorf("interval_ms: %w", err)
		}
		if v <= 0 {
			return fmt.Errorf("interval_ms must be > 0, got %d", v)
		}
		cfg.Compaction.IntervalMs = v

	case "compaction.tombstone_grace_ms":
		v, err := toInt64(raw)
		if err != nil {
			return fmt.Errorf("tombstone_grace_ms: %w", err)
		}
		if v < 0 {
			return fmt.Errorf("tombstone_grace_ms must be >= 0, got %d", v)
		}
		cfg.Compaction.TombstoneGraceMs = v

	case "drain_timeout_ms":
		v, err := toInt(raw)
		if err != nil {
			return fmt.Errorf("drain_timeout_ms: %w", err)
		}
		if v <= 0 {
			return fmt.Errorf("drain_timeout_ms must be > 0, got %d", v)
		}
		cfg.DrainTimeoutMs = v

	case "flow_control_pause_ms":
		v, err := toInt(raw)
		if err != nil {
			return fmt.Errorf("flow_control_pause_ms: %w", err)
		}
		if v < 0 {
			return fmt.Errorf("flow_control_pause_ms must be >= 0, got %d", v)
		}
		cfg.FlowControlPauseMs = v

	case "logging.level":
		v, ok := raw.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", raw)
		}
		switch strings.ToLower(v) {
		case "debug", "info", "warn", "error":
			cfg.Logging.Level = strings.ToLower(v)
		default:
			return fmt.Errorf("logging.level must be one of debug/info/warn/error, got %q", v)
		}

	default:
		return fmt.Errorf("field %q is not hot-reloadable and requires a restart", fieldPath)
	}
	return nil
}

// toInt converts a JSON number (float64) or an int to an int, returning an
// error for other types.
func toInt(raw interface{}) (int, error) {
	switch v := raw.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case int64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("expected numeric, got %T", raw)
	}
}

// toInt64 converts a JSON number (float64) or an int/int64 to int64.
func toInt64(raw interface{}) (int64, error) {
	switch v := raw.(type) {
	case float64:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("expected numeric, got %T", raw)
	}
}

// Validate returns an error if the Config violates any invariant.
// It delegates to the package-level validate function.
func Validate(cfg *Config) error {
	return validate(cfg)
}

// DurationMs returns a time.Duration from a millisecond value.
func DurationMs(ms int) time.Duration {
	return time.Duration(ms) * time.Millisecond
}
