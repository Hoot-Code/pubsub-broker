package broker

import (
	"encoding/json"
	"net/http"

	"github.com/Hoot-Code/pubsub-broker/internal/auth"
)

// configField wraps a value with a hot_reloadable annotation for the frontend.
type configField struct {
	Value         interface{} `json:"value"`
	HotReloadable bool        `json:"hot_reloadable"`
}

// httpConfigEffective implements GET /config/effective — returns the broker's
// current effective configuration as JSON, read-only. Admin role only. Secret
// values (API key plaintext) are redacted. Each field is annotated with
// hot_reloadable so the frontend can render edit controls accordingly.
func (b *Broker) httpConfigEffective(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	id := IdentityFromContext(r.Context())
	if id == nil || !id.Can(string(auth.PermAdmin), "") {
		w.WriteHeader(http.StatusForbidden)
		_ = writeJSON(w, map[string]string{"error": "admin role required"})
		return
	}

	cfg := b.cfg.Get()
	// Build a safe copy that excludes secret material.
	type apiKeySafe struct {
		ClientID string   `json:"client_id"`
		Role     string   `json:"role"`
		Topics   []string `json:"topics,omitempty"`
	}
	type authSafe struct {
		Enabled bool         `json:"enabled"`
		APIKeys []apiKeySafe `json:"api_keys"`
	}
	keys := make([]apiKeySafe, len(cfg.Auth.APIKeys))
	for i, k := range cfg.Auth.APIKeys {
		keys[i] = apiKeySafe{
			ClientID: k.ClientID,
			Role:     k.Role,
			Topics:   k.Topics,
		}
	}
	type networkSafe struct {
		Host                             string `json:"host"`
		Port                             int    `json:"port"`
		MaxConnections                   int    `json:"max_connections"`
		TLSCertFile                      string `json:"tls_cert_file"`
		TLSMinVersion                    uint16 `json:"tls_min_version"`
		PprofEnabled                     bool   `json:"pprof_enabled"`
		DashboardEnabled                 bool   `json:"dashboard_enabled"`
		ExplorerEnabled                  bool   `json:"explorer_enabled"`
		ExplorerMaxConnections           int    `json:"explorer_max_connections"`
		DashboardAuthEnabled             *bool  `json:"dashboard_auth_enabled,omitempty"`
		DashboardSessionTTL              string `json:"dashboard_session_ttl"`
		DashboardLoginRateLimitPerMinute int    `json:"dashboard_login_rate_limit_per_minute"`
	}

	safe := map[string]interface{}{
		"broker": cfg.Broker,
		"network": networkSafe{
			Host:                             cfg.Network.Host,
			Port:                             cfg.Network.Port,
			MaxConnections:                   cfg.Network.MaxConnections,
			TLSCertFile:                      cfg.Network.TLSCertFile,
			TLSMinVersion:                    cfg.Network.TLSMinVersion,
			PprofEnabled:                     cfg.Network.PprofEnabled,
			DashboardEnabled:                 cfg.Network.DashboardEnabled,
			ExplorerEnabled:                  cfg.Network.ExplorerEnabled,
			ExplorerMaxConnections:           cfg.Network.ExplorerMaxConnections,
			DashboardAuthEnabled:             cfg.Network.DashboardAuthEnabled,
			DashboardSessionTTL:              cfg.Network.DashboardSessionTTL.String(),
			DashboardLoginRateLimitPerMinute: cfg.Network.DashboardLoginRateLimitPerMinute,
		},
		"replication": cfg.Replication,
		"storage":     cfg.Storage,
		"auth":        authSafe{Enabled: cfg.Auth.Enabled, APIKeys: keys},
		"logging":     cfg.Logging,
		"cluster":     cfg.Cluster,
		"gateway":     cfg.Gateway,
	}

	// Annotate hot-reloadable fields with hot_reloadable flag.
	safe["retention"] = map[string]interface{}{
		"max_age_hours": configField{Value: cfg.Retention.MaxAgeHours, HotReloadable: true},
		"max_size_mb":   configField{Value: cfg.Retention.MaxSizeMB, HotReloadable: true},
	}
	safe["rate_limit"] = map[string]interface{}{
		"enabled":          configField{Value: cfg.RateLimit.Enabled, HotReloadable: true},
		"per_client_rps":   configField{Value: cfg.RateLimit.PerClientRPS, HotReloadable: true},
		"per_topic_rps":    configField{Value: cfg.RateLimit.PerTopicRPS, HotReloadable: true},
		"burst_multiplier": configField{Value: cfg.RateLimit.BurstMultiplier, HotReloadable: false},
	}
	safe["compaction"] = map[string]interface{}{
		"interval_ms":        configField{Value: cfg.Compaction.IntervalMs, HotReloadable: true},
		"tombstone_grace_ms": configField{Value: cfg.Compaction.TombstoneGraceMs, HotReloadable: true},
	}
	safe["drain_timeout_ms"] = configField{Value: cfg.DrainTimeoutMs, HotReloadable: true}
	safe["flow_control_pause_ms"] = configField{Value: cfg.FlowControlPauseMs, HotReloadable: true}
	if lg := cfg.Logging.Level; lg != "" {
		safe["logging"] = map[string]interface{}{
			"level":  configField{Value: cfg.Logging.Level, HotReloadable: true},
			"format": configField{Value: cfg.Logging.Format, HotReloadable: false},
		}
	}

	_ = json.NewEncoder(w).Encode(safe)
}
