package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

// configPatchRequest is the JSON body for PATCH /config.
type configPatchRequest struct {
	Changes map[string]interface{} `json:"changes"`
}

// configPatchResponse is the JSON response for PATCH /config.
type configPatchResponse struct {
	Changes  map[string]interface{} `json:"changes,omitempty"`
	Error    string                 `json:"error,omitempty"`
	Rejected []string               `json:"rejected_fields,omitempty"`
}

// configPatchRateLimiter enforces a per-identity rate limit on config patches
// to prevent abuse. Default: 5 changes per minute per identity.
type configPatchRateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*configPatchBucket
}

type configPatchBucket struct {
	count       int
	windowStart time.Time
}

// newConfigPatchRateLimiter creates a new rate limiter for config patch
// requests.
func newConfigPatchRateLimiter() *configPatchRateLimiter {
	return &configPatchRateLimiter{
		buckets: make(map[string]*configPatchBucket),
	}
}

// Allow reports whether the given identity is permitted one more config patch
// within the current 1-minute window. Resets the window after 60 seconds.
func (r *configPatchRateLimiter) Allow(identity string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	b, ok := r.buckets[identity]
	if !ok || now.Sub(b.windowStart) > 60*time.Second {
		r.buckets[identity] = &configPatchBucket{
			count:       1,
			windowStart: now,
		}
		return true
	}
	if b.count >= 5 {
		return false
	}
	b.count++
	return true
}

// httpConfigPatch implements PATCH /config — hot-reload a whitelist of config
// fields. Admin role only. The patch is validated against the whitelist, then
// against config.Validate(), then applied to live components and persisted to
// disk before returning 200.
func (b *Broker) httpConfigPatch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	id := IdentityFromContext(r.Context())
	if id == nil || !id.Can(string(auth.PermAdmin), "") {
		w.WriteHeader(http.StatusForbidden)
		_ = writeJSON(w, map[string]string{"error": "admin role required"})
		return
	}

	// Rate limit.
	identityKey := id.ClientID
	if identityKey == "" {
		identityKey = "anonymous"
	}
	if !b.configPatchLimiter.Allow(identityKey) {
		w.WriteHeader(http.StatusTooManyRequests)
		_ = writeJSON(w, map[string]string{"error": "rate limit exceeded: max 5 config changes per minute"})
		return
	}

	var req configPatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = writeJSON(w, map[string]string{"error": "invalid request body"})
		return
	}
	if len(req.Changes) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		_ = writeJSON(w, map[string]string{"error": "changes is required and must not be empty"})
		return
	}

	// Check all keys against the whitelist — collect ALL rejections.
	var rejected []string
	for fieldPath := range req.Changes {
		if !config.IsHotReloadable(fieldPath) {
			rejected = append(rejected, fieldPath)
		}
	}
	if len(rejected) > 0 {
		w.WriteHeader(http.StatusBadRequest)
		_ = writeJSON(w, configPatchResponse{
			Error:    "non-hot-reloadable fields require a restart",
			Rejected: rejected,
		})
		return
	}

	// Apply patch to a candidate config.
	currentCfg := b.cfg.Get()
	candidate, err := config.ApplyPatch(*currentCfg, req.Changes)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = writeJSON(w, configPatchResponse{Error: err.Error()})
		return
	}

	// Validate the full candidate config before applying anything.
	if err := config.Validate(&candidate); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = writeJSON(w, configPatchResponse{Error: fmt.Sprintf("validation: %v", err)})
		return
	}

	// Build audit details with old/new values before applying.
	details := make(map[string]string)
	details["source"] = "dashboard"
	if id.ClientID != "" {
		details["source"] = "api"
	}
	for fieldPath, newVal := range req.Changes {
		oldVal := getFieldValue(currentCfg, fieldPath)
		details[fieldPath+"_old"] = fmt.Sprintf("%v", oldVal)
		details[fieldPath+"_new"] = fmt.Sprintf("%v", newVal)
	}

	// Apply to live components (Part C).
	b.applyConfigToLive(&candidate)

	// Update the loader's in-memory snapshot.
	b.cfg.Set(&candidate)

	// Persist to disk.
	cfgPath := b.cfg.Path()
	if cfgPath != "" {
		if err := persistConfig(cfgPath, &candidate); err != nil {
			b.log.Warn("config patch: failed to persist to disk", "err", err)
		}
	}

	// Audit log.
	if b.audit != nil {
		b.audit.Log(audit.Event{
			Type:     audit.EventConfigReload,
			ClientID: id.ClientID,
			Success:  true,
			Details:  details,
		})
	}

	// Return the effective config (redacted).
	b.httpConfigEffective(w, r)
}

// applyConfigToLive pushes changed settings to running components.
func (b *Broker) applyConfigToLive(cfg *config.Config) {
	b.authMu.Lock()
	b.rateLimiter.UpdateLimits(&cfg.RateLimit)
	b.authMu.Unlock()

	b.topics.UpdateRetentionConfig(cfg.Retention.MaxAgeHours, cfg.Retention.MaxSizeMB)

	if b.compactor != nil {
		b.compactor.UpdateConfig(
			config.DurationMs(cfg.Compaction.IntervalMs),
			int64(cfg.Compaction.TombstoneGraceMs),
		)
	}

	b.log.SetLevel(cfg.Logging.Level)

	// Update atomic config values for live component use.
	b.drainTimeoutMs.Store(int64(cfg.DrainTimeoutMs))
	b.flowControlPauseMs.Store(int64(cfg.FlowControlPauseMs))
}

// persistConfig writes the full config to path using an atomic temp-file +
// rename pattern (matching wal_topic.go's convention).
func persistConfig(path string, cfg *config.Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// getFieldValue returns the string representation of a dotted config field.
func getFieldValue(cfg *config.Config, fieldPath string) interface{} {
	switch fieldPath {
	case "rate_limit.enabled":
		return cfg.RateLimit.Enabled
	case "rate_limit.per_client_rps":
		return cfg.RateLimit.PerClientRPS
	case "rate_limit.per_topic_rps":
		return cfg.RateLimit.PerTopicRPS
	case "retention.max_age_hours":
		return cfg.Retention.MaxAgeHours
	case "retention.max_size_mb":
		return cfg.Retention.MaxSizeMB
	case "compaction.interval_ms":
		return cfg.Compaction.IntervalMs
	case "compaction.tombstone_grace_ms":
		return cfg.Compaction.TombstoneGraceMs
	case "drain_timeout_ms":
		return cfg.DrainTimeoutMs
	case "flow_control_pause_ms":
		return cfg.FlowControlPauseMs
	case "logging.level":
		return cfg.Logging.Level
	default:
		return nil
	}
}

// Ensure fmt is used.
var _ = fmt.Sprintf
