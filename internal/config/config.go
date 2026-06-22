// Package config handles broker configuration loading and hot-reloading.
// Config files use JSON format (zero external deps). See configs/broker.json.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// ─── Config Structures ───────────────────────────────────────────────────────

// Config is the root configuration for the broker.
type Config struct {
	Broker      BrokerConfig      `json:"broker"`
	Network     NetworkConfig     `json:"network"`
	Replication ReplicationConfig `json:"replication"`
	Storage     StorageConfig     `json:"storage"`
	Retention   RetentionConfig   `json:"retention"`
	Auth        AuthConfig        `json:"auth"`
	RateLimit   RateLimitConfig   `json:"rate_limit"`
	Logging     LoggingConfig     `json:"logging"`
	// Cluster configures optional cluster membership and leader election.
	Cluster ClusterConfig `json:"cluster"`
	// Compaction configures the key-based log-compaction background loop.
	Compaction CompactionConfig `json:"compaction"`
	// Gateway configures the optional embedded HTTP/WebSocket gateway.
	Gateway GatewayConfig `json:"gateway"`
	// FlowControlPauseMs is the number of milliseconds the broker pauses
	// delivery to a push consumer whose Messages() channel is full before
	// moving the message to the DLQ. 0 uses the default of 100 ms.
	FlowControlPauseMs int `json:"flow_control_pause_ms"`
	// DrainTimeoutMs is the number of milliseconds Stop() waits for
	// in-flight requests to complete before forcefully closing connections.
	// Default: 5000. 0 uses the default.
	DrainTimeoutMs int `json:"drain_timeout_ms"`
	// AuditLogFile is the path to the append-only audit log file.
	// When non-empty the broker opens the file and writes one JSON event per
	// line for every authenticated action. The file is created if absent.
	AuditLogFile string `json:"audit_log_file"`
}

// BrokerConfig holds node-level settings.
type BrokerConfig struct {
	NodeID string `json:"node_id"`
}

// NetworkConfig holds TCP server settings.
type NetworkConfig struct {
	Host           string        `json:"host"`
	Port           int           `json:"port"`
	MaxConnections int           `json:"max_connections"`
	ReadTimeout    time.Duration `json:"read_timeout"`
	WriteTimeout   time.Duration `json:"write_timeout"`
	IdleTimeout    time.Duration `json:"idle_timeout"`
	// TLSCertFile is the path to the TLS certificate file (PEM).
	// When non-empty, TLS is enabled for both the TCP and HTTP servers.
	TLSCertFile string `json:"tls_cert_file"`
	// TLSKeyFile is the path to the TLS private key file (PEM).
	TLSKeyFile string `json:"tls_key_file"`
	// TLSMinVersion is the minimum TLS version accepted. Defaults to
	// tls.VersionTLS13 (0x0304) when zero.
	TLSMinVersion uint16 `json:"tls_min_version"`
	// PprofEnabled enables the /debug/pprof/ profiling endpoint on the
	// HTTP admin server. When false (default), requests to /debug/pprof/
	// return 403 Forbidden.
	PprofEnabled bool `json:"pprof_enabled"`
	// DashboardEnabled enables the embedded dashboard at GET /dashboard.
	// When false, requests to /dashboard return 403 Forbidden.
	// Default: true (the dashboard is a read-only view with no write-path
	// side effects beyond /dlq/replay which is already exposed).
	DashboardEnabled bool `json:"dashboard_enabled"`
	// ExplorerEnabled enables the Live Message Explorer WebSocket endpoint
	// at GET /explorer/stream. When false (default: true), requests return
	// 403 Forbidden. Default is true because the Explorer is a read-only
	// live tail with no write-path side effects — same risk profile as the
	// dashboard, unlike PprofEnabled which is default-false because it
	// exposes detailed runtime internals that could aid an attacker.
	ExplorerEnabled bool `json:"explorer_enabled"`
	// ExplorerMaxConnections caps the number of concurrent Explorer
	// WebSocket connections. Once this limit is reached, new upgrade
	// attempts return 503 Service Unavailable. Default: 50.
	ExplorerMaxConnections int `json:"explorer_max_connections"`
	// DashboardAuthEnabled overrides the computed dashboard auth behaviour.
	// When non-nil and set to false, dashboard authentication is forced OFF
	// even when auth.Enabled is true (for trusted-network deployments).
	// It can never force auth ON when auth.Enabled is false.
	DashboardAuthEnabled *bool `json:"dashboard_auth_enabled,omitempty"`
	// DashboardSessionTTL is how long a dashboard session cookie is valid.
	// Default: 12h. Zero uses the default.
	DashboardSessionTTL time.Duration `json:"dashboard_session_ttl"`
	// DashboardLoginRateLimitPerMinute caps login attempts per remote IP.
	// Default: 10. Zero uses the default.
	DashboardLoginRateLimitPerMinute int `json:"dashboard_login_rate_limit_per_minute"`
}

// ReplicationConfig controls leader-follower replication behaviour.
type ReplicationConfig struct {
	Factor       int           `json:"factor"`
	SyncInterval time.Duration `json:"sync_interval"`
	AckTimeout   time.Duration `json:"ack_timeout"`
}

// StorageConfig controls the on-disk storage engine.
type StorageConfig struct {
	WALPath            string `json:"wal_path"`
	DataPath           string `json:"data_path"`
	SegmentMaxBytes    int64  `json:"segment_max_bytes"`
	IndexIntervalBytes int64  `json:"index_interval_bytes"`
	// SyncPolicy controls when segment data is flushed to disk.
	// "always"   – call fsync after every Write (safest, slowest).
	// "interval" – call fsync every 100 ms via a background ticker.
	// "os"       – rely entirely on the OS page-cache flush.
	SyncPolicy string `json:"sync_policy"`
	// WalBackpressureThreshold is the maximum number of pending WAL bytes
	// allowed before handlePublish rejects new writes with BROKER_OVERLOADED.
	// 0 disables backpressure (default).
	WalBackpressureThreshold int64 `json:"wal_backpressure_threshold"`
}

// RetentionConfig controls message retention policy.
type RetentionConfig struct {
	MaxAgeHours int   `json:"max_age_hours"`
	MaxSizeMB   int64 `json:"max_size_mb"`
}

// CompactionConfig controls the key-based log-compaction background loop.
// It only applies to topics whose TopicConfig.CompactionMode == "compact";
// age/size retention (RetentionConfig) is skipped for those topics.
type CompactionConfig struct {
	// IntervalMs is the period between compaction sweeps, in milliseconds.
	// Default: 60000 (60 s) when zero.
	IntervalMs int `json:"interval_ms"`
	// TombstoneGraceMs is how long a tombstone record (and all older
	// records for its key) is retained after it is written, in
	// milliseconds, before being purged on the next sweep. This gives slow
	// consumers time to observe the deletion. Default: 86400000 (24 h).
	TombstoneGraceMs int64 `json:"tombstone_grace_ms"`
}

// GatewayConfig controls the optional embedded HTTP/WebSocket gateway that
// lets browsers and languages without an SDK publish/subscribe over plain
// HTTP and WebSocket instead of the binary protocol.
type GatewayConfig struct {
	// Enabled starts the gateway as part of the broker process when true.
	// Default: false.
	Enabled bool `json:"enabled"`
	// Addr is the listen address for the gateway HTTP server, e.g. ":8080".
	Addr string `json:"addr"`
}

// AuthConfig holds API key authentication settings.
type AuthConfig struct {
	Enabled bool          `json:"enabled"`
	APIKeys []APIKeyEntry `json:"api_keys"`
}

// APIKeyEntry holds a single API key and its RBAC role.
// The legacy Permissions field is accepted for backward compatibility;
// if Role is empty and Permissions is set, the authenticator derives the
// role from the permission list.
type APIKeyEntry struct {
	Key      string `json:"key"`
	ClientID string `json:"client_id"`
	// Role is the RBAC role for this key: "admin", "producer", "consumer", or "viewer".
	Role string `json:"role,omitempty"`
	// Topics is an optional allowlist of topic names this key may access.
	// An empty list means all topics are permitted.
	Topics []string `json:"topics,omitempty"`
	// Permissions is the legacy field. Use Role instead.
	Permissions []string `json:"permissions,omitempty"`
}

// RateLimitConfig controls per-client and per-topic rate limits.
type RateLimitConfig struct {
	Enabled         bool `json:"enabled"`
	PerClientRPS    int  `json:"per_client_rps"`
	PerTopicRPS     int  `json:"per_topic_rps"`
	BurstMultiplier int  `json:"burst_multiplier"`
}

// LoggingConfig controls log output format and level.
type LoggingConfig struct {
	Level  string `json:"level"`  // debug, info, warn, error
	Format string `json:"format"` // json, text
}

// ClusterConfig holds settings for cluster membership and leader election.
// When Enabled is false (the default), the broker runs in single-node mode
// and all cluster code paths are no-ops.
type ClusterConfig struct {
	// Enabled activates cluster mode. Default: false (single-node).
	Enabled bool `json:"enabled"`
	// NodeID is the unique, stable identifier for this node in the cluster.
	NodeID string `json:"node_id"`
	// BindAddr is the host:port for inter-node TCP communication.
	BindAddr string `json:"bind_addr"`
	// Seeds is the list of seed node addresses to contact on startup.
	Seeds []string `json:"seeds"`
	// HeartbeatInterval is the period between leader heartbeats, in milliseconds.
	// Default: 150.
	HeartbeatInterval int `json:"heartbeat_interval_ms"`
	// ElectionTimeoutMin is the lower bound of the randomised election timeout,
	// in milliseconds. Default: 450.
	ElectionTimeoutMin int `json:"election_timeout_min_ms"`
	// ElectionTimeoutMax is the upper bound of the randomised election timeout,
	// in milliseconds. Default: 750.
	ElectionTimeoutMax int `json:"election_timeout_max_ms"`
	// QuorumTimeoutMs is the maximum time in milliseconds the broker will wait
	// for a quorum of ISR replicas to acknowledge a published message before
	// returning QUORUM_TIMEOUT.  Default: 5000.
	QuorumTimeoutMs int `json:"quorum_timeout_ms"`
	// ConsensusAlgorithm selects the consensus mechanism for leader election.
	// "bully" (default) uses the existing Bully algorithm for backward
	// compatibility. "raft" uses the new Raft implementation which provides
	// split-brain safety that Bully cannot guarantee. New deployments should
	// set "raft".
	ConsensusAlgorithm string `json:"consensus_algorithm"`
	// RaftDataDir is the directory where Raft persistent state (vote, log)
	// is stored. Defaults to <data_dir>/raft when empty.
	RaftDataDir string `json:"raft_data_dir"`
	// MTLSCertFile is the path to the PEM-encoded TLS certificate for this
	// node. When all three mTLS fields are set, inter-node communication is
	// secured with mutual TLS. If any field is empty, plain TCP is used.
	MTLSCertFile string `json:"mtls_cert_file"`
	// MTLSKeyFile is the path to the PEM-encoded private key for MTLSCertFile.
	MTLSKeyFile string `json:"mtls_key_file"`
	// MTLSCAFile is the path to the PEM-encoded CA certificate used to verify
	// peer node certificates.
	MTLSCAFile string `json:"mtls_ca_file"`
}

// ─── Loader ──────────────────────────────────────────────────────────────────

// Loader manages config loading and hot-reload.
type Loader struct {
	mu        sync.RWMutex
	path      string
	current   *Config
	modTime   time.Time
	onChange  []func(*Config)
	stopC     chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

// Load reads the config from path and starts hot-reload polling.
func Load(path string) (*Loader, error) {
	l := &Loader{
		path:    path,
		stopC:   make(chan struct{}),
		stopped: make(chan struct{}),
	}
	cfg, modTime, err := l.readFile()
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	l.current = cfg
	l.modTime = modTime
	go l.watchLoop()
	return l, nil
}

// Get returns the current (always up-to-date) configuration snapshot.
func (l *Loader) Get() *Config {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.current
}

// Path returns the file path this loader was created from.
func (l *Loader) Path() string {
	return l.path
}

// Set atomically replaces the current config snapshot. It is intended for the
// PATCH /config handler to push validated in-memory updates without going
// through the file watcher. Callbacks are NOT fired — the caller is
// responsible for wiring live components.
func (l *Loader) Set(cfg *Config) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.current = cfg
}

// OnChange registers a callback invoked on successful config reload.
// Callbacks run sequentially in the hot-reload goroutine — keep them fast.
func (l *Loader) OnChange(fn func(*Config)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onChange = append(l.onChange, fn)
}

// Close stops the hot-reload watcher. Safe to call more than once.
func (l *Loader) Close() {
	l.closeOnce.Do(func() {
		close(l.stopC)
		<-l.stopped
	})
}

func (l *Loader) watchLoop() {
	defer close(l.stopped)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-l.stopC:
			return
		case <-ticker.C:
			l.reload()
		}
	}
}

func (l *Loader) reload() {
	cfg, modTime, err := l.readFile()
	if err != nil {
		return
	}
	l.mu.RLock()
	unchanged := !modTime.After(l.modTime)
	l.mu.RUnlock()
	if unchanged {
		return
	}
	l.mu.Lock()
	l.current = cfg
	l.modTime = modTime
	callbacks := l.onChange
	l.mu.Unlock()

	for _, fn := range callbacks {
		fn(cfg)
	}
}

func (l *Loader) readFile() (*Config, time.Time, error) {
	info, err := os.Stat(l.path)
	if err != nil {
		return nil, time.Time{}, err
	}
	data, err := os.ReadFile(l.path)
	if err != nil {
		return nil, time.Time{}, err
	}
	cfg := defaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, time.Time{}, fmt.Errorf("parse: %w", err)
	}
	if err := validate(cfg); err != nil {
		return nil, time.Time{}, fmt.Errorf("validate: %w", err)
	}
	return cfg, info.ModTime(), nil
}

// GenerateDefault returns the fully annotated default configuration as a
// map[string]interface{} suitable for JSON encoding. This is the single
// source of truth for default config values — both cmd/gen-config and
// the config_sync_test call this function to guarantee they stay in sync.
func GenerateDefault() map[string]interface{} {
	return map[string]interface{}{
		"_comment_broker": "Node-level identification settings.",
		"broker": map[string]interface{}{
			"_comment_node_id": "Unique ID for this broker in a cluster. Must be stable across restarts.",
			"node_id":          "node-1",
		},
		"_comment_network": "TCP listener and optional TLS settings.",
		"network": map[string]interface{}{
			"_comment_host":              "Interface to bind. '0.0.0.0' for all, '127.0.0.1' for loopback only.",
			"host":                       "0.0.0.0",
			"_comment_port":              "TCP port. Use 0 to let the OS assign an ephemeral port (useful for tests).",
			"_comment_admin_port":        "The HTTP admin port is always the TCP port + 1 (9001 by default).",
			"port":                       9000,
			"_comment_max_connections":   "Maximum concurrent client connections. Excess connections are rejected.",
			"max_connections":            10000,
			"_comment_read_timeout":      "Per-frame read deadline in nanoseconds (30 s = 30000000000).",
			"read_timeout":               30000000000,
			"_comment_write_timeout":     "Per-frame write deadline in nanoseconds.",
			"write_timeout":              30000000000,
			"_comment_idle_timeout":      "Idle connection timeout in nanoseconds (5 min = 300000000000).",
			"idle_timeout":               300000000000,
			"_comment_tls_cert_file":     "Path to TLS certificate (PEM). Leave empty to disable TLS.",
			"tls_cert_file":              "",
			"_comment_tls_key_file":      "Path to TLS private key (PEM). Required when tls_cert_file is set.",
			"tls_key_file":               "",
			"_comment_tls_min_version":   "Minimum TLS version. 0 = default TLS 1.3 (0x0304). Set 0x0303 for TLS 1.2.",
			"tls_min_version":            0,
			"_comment_pprof_enabled":     "Enable /debug/pprof/ profiling endpoint on the HTTP admin server.",
			"pprof_enabled":              false,
			"_comment_dashboard_enabled": "Enable the embedded dashboard at GET /dashboard. Default: true.",
			"dashboard_enabled":          true,
			"_comment_explorer_enabled":  "Enable Live Message Explorer WS endpoint. Default: true (read-only live tail, same risk profile as dashboard).",
			"explorer_enabled":           true,
			"_comment_explorer_max_conn": "Max concurrent Explorer WebSocket connections. 503 when exceeded. Default: 50.",
			"explorer_max_connections":   50,
		},
		"_comment_replication": "Leader-follower replication parameters.",
		"replication": map[string]interface{}{
			"_comment_factor":        "Number of replicas per partition including the leader. Min: 1.",
			"factor":                 1,
			"_comment_sync_interval": "Follower pull heartbeat interval in nanoseconds (100 ms = 100000000).",
			"sync_interval":          100000000,
			"_comment_ack_timeout":   "Follower ack timeout in nanoseconds before marking out-of-sync.",
			"ack_timeout":            5000000000,
		},
		"_comment_storage": "On-disk storage engine settings.",
		"storage": map[string]interface{}{
			"_comment_wal_path":             "Directory for the write-ahead log. The offset WAL is stored at <wal_path>.offsets.",
			"wal_path":                      "./data/wal",
			"_comment_data_path":            "Root directory for segment files (one sub-directory per topic-partition).",
			"data_path":                     "./data/segments",
			"_comment_segment_max_bytes":    "Maximum segment file size in bytes before rollover. Default: 1 GiB.",
			"segment_max_bytes":             1073741824,
			"_comment_index_interval_bytes": "Sparse index granularity in bytes. Smaller = faster seeks, more disk.",
			"index_interval_bytes":          4096,
			"_comment_sync_policy": "Fsync strategy: 'always' (safest), 'interval' (default, 100 ms ticker), " +
				"'os' (no fsync, risk of data loss on power failure).",
			"sync_policy": "interval",
		},
		"_comment_retention": "Message retention policy applied by the background compaction loop.",
		"retention": map[string]interface{}{
			"_comment_max_age_hours": "Delete segments whose oldest record exceeds this age in hours.",
			"max_age_hours":          24,
			"_comment_max_size_mb":   "Delete oldest non-active segments when total partition size exceeds this (MiB).",
			"max_size_mb":            1024,
		},
		"_comment_auth": "API key authentication. Keys are stored as SHA-256 hashes; plaintext is not retained.",
		"auth": map[string]interface{}{
			"_comment_enabled":  "Set true to require an API key on every connection.",
			"enabled":           false,
			"_comment_api_keys": "List of API keys. Each grants a set of permissions to a named client.",
			"api_keys": []map[string]interface{}{
				{
					"_comment_key":         "Raw API key. Hashed at startup; never stored in plaintext.",
					"key":                  "change-me-to-a-strong-random-secret",
					"_comment_client_id":   "Human-readable label used in audit logs.",
					"client_id":            "service-account-1",
					"_comment_permissions": "Allowed operations: 'publish', 'subscribe', 'admin' (implies all).",
					"permissions":          []string{"publish", "subscribe"},
				},
			},
		},
		"_comment_rate_limit": "Token-bucket rate limiting with automatic cleanup of idle buckets.",
		"rate_limit": map[string]interface{}{
			"_comment_enabled":          "Set true to enforce rate limits.",
			"enabled":                   false,
			"_comment_per_client_rps":   "Maximum requests per second from one client.",
			"per_client_rps":            10000,
			"_comment_per_topic_rps":    "Maximum requests per second targeting one topic.",
			"per_topic_rps":             50000,
			"_comment_burst_multiplier": "Burst capacity = rate × multiplier. Must be >= 1.",
			"burst_multiplier":          2,
		},
		"_comment_logging": "Structured logging configuration.",
		"logging": map[string]interface{}{
			"_comment_level":  "Minimum log level: 'debug', 'info', 'warn', 'error'.",
			"level":           "info",
			"_comment_format": "Log format: 'json' (machine-readable) or 'text' (human-readable).",
			"format":          "json",
		},
		"_comment_cluster": "Cluster membership and leader election settings.",
		"cluster": map[string]interface{}{
			"_comment_enabled":              "Activate cluster mode. Default: false (single-node).",
			"enabled":                       false,
			"_comment_node_id":              "Unique stable identifier for this node in the cluster.",
			"node_id":                       "",
			"_comment_bind_addr":            "host:port for inter-node TCP communication.",
			"bind_addr":                     "",
			"_comment_seeds":                "List of seed node addresses to contact on startup.",
			"seeds":                         []string{},
			"_comment_heartbeat_interval":   "Leader heartbeat period in ms. Default: 150.",
			"heartbeat_interval_ms":         150,
			"_comment_election_timeout_min": "Lower bound of randomised election timeout in ms. Default: 450.",
			"election_timeout_min_ms":       450,
			"_comment_election_timeout_max": "Upper bound of randomised election timeout in ms. Default: 750.",
			"election_timeout_max_ms":       750,
			"_comment_quorum_timeout_ms":    "Max wait (ms) for ISR quorum ack. Default: 5000.",
			"quorum_timeout_ms":             5000,
			"_comment_consensus_algorithm":  "Consensus mechanism: 'bully' (default) or 'raft'.",
			"consensus_algorithm":           "bully",
			"_comment_raft_data_dir":        "Directory for Raft persistent state. Defaults to <data_dir>/raft.",
			"raft_data_dir":                 "",
			"_comment_mtls_cert_file":       "PEM certificate for mutual TLS between nodes. Plain TCP if any field is empty.",
			"mtls_cert_file":                "",
			"_comment_mtls_key_file":        "PEM private key for mtls_cert_file.",
			"mtls_key_file":                 "",
			"_comment_mtls_ca_file":         "PEM CA certificate for verifying peer node certificates.",
			"mtls_ca_file":                  "",
		},
		"_comment_compaction": "Key-based log-compaction background loop.",
		"compaction": map[string]interface{}{
			"_comment_interval_ms":        "Compaction sweep period in ms. Default: 60000 (60 s).",
			"interval_ms":                 60000,
			"_comment_tombstone_grace_ms": "Tombstone retention in ms before purge. Default: 86400000 (24 h).",
			"tombstone_grace_ms":          86400000,
		},
		"_comment_gateway": "Embedded HTTP/WebSocket gateway for browsers and SDK-less clients.",
		"gateway": map[string]interface{}{
			"_comment_enabled": "Start the gateway as part of the broker process. Default: false.",
			"enabled":          false,
			"_comment_addr":    "Listen address for the gateway HTTP server, e.g. ':8080'.",
			"addr":             ":8080",
		},
		"_comment_flow_control_pause_ms": "Pause (ms) before moving a message to the DLQ when a push consumer channel is full. 0 = default (100 ms).",
		"flow_control_pause_ms":          0,
		"_comment_drain_timeout_ms":      "Max wait (ms) for in-flight requests during Stop(). 0 = default (5000 ms).",
		"drain_timeout_ms":               0,
		"_comment_audit_log_file":        "Path to append-only audit log file. Empty = disabled.",
		"audit_log_file":                 "",
	}
}

// defaultConfig returns a config populated with safe defaults.
func defaultConfig() *Config {
	return &Config{
		Broker: BrokerConfig{NodeID: "node-1"},
		Network: NetworkConfig{
			Host:                             "0.0.0.0",
			Port:                             9000,
			MaxConnections:                   10000,
			ReadTimeout:                      30 * time.Second,
			WriteTimeout:                     30 * time.Second,
			IdleTimeout:                      5 * time.Minute,
			DashboardEnabled:                 true,
			ExplorerEnabled:                  true,
			DashboardSessionTTL:              12 * time.Hour,
			DashboardLoginRateLimitPerMinute: 10,
		},
		Replication: ReplicationConfig{
			Factor:       1,
			SyncInterval: 100 * time.Millisecond,
			AckTimeout:   5 * time.Second,
		},
		Storage: StorageConfig{
			WALPath:            "./data/wal",
			DataPath:           "./data/segments",
			SegmentMaxBytes:    1 << 30,
			IndexIntervalBytes: 4096,
			SyncPolicy:         "interval",
		},
		Retention: RetentionConfig{
			MaxAgeHours: 24,
			MaxSizeMB:   1024,
		},
		Auth: AuthConfig{Enabled: false},
		RateLimit: RateLimitConfig{
			Enabled:         false,
			PerClientRPS:    10000,
			PerTopicRPS:     50000,
			BurstMultiplier: 2,
		},
		Logging: LoggingConfig{Level: "info", Format: "json"},
		Cluster: ClusterConfig{
			Enabled:            false,
			HeartbeatInterval:  150,
			ElectionTimeoutMin: 450,
			ElectionTimeoutMax: 750,
		},
		Compaction: CompactionConfig{
			IntervalMs:       60000,
			TombstoneGraceMs: 86400000,
		},
		Gateway: GatewayConfig{
			Enabled: false,
		},
	}
}

func validate(cfg *Config) error {
	// The broker always allocates an HTTP admin port at tcpPort+1.
	// A TCP port of 65535 would overflow to 65536 (invalid), so the upper bound
	// is 65534. Port 0 remains valid (OS-assigned ephemeral, used by tests).
	if cfg.Network.Port < 0 || cfg.Network.Port > 65534 {
		return fmt.Errorf("network.port must be between 0 and 65534 (0 = OS-assigned; 65535 reserved for HTTP admin), got %d", cfg.Network.Port)
	}
	if cfg.Replication.Factor < 1 {
		return fmt.Errorf("replication.factor must be >= 1")
	}
	if cfg.Storage.WALPath == "" {
		return fmt.Errorf("storage.wal_path must not be empty")
	}
	if cfg.Storage.DataPath == "" {
		return fmt.Errorf("storage.data_path must not be empty")
	}
	switch cfg.Storage.SyncPolicy {
	case "", "interval", "always", "os":
		// valid
	default:
		return fmt.Errorf("storage.sync_policy must be \"always\", \"interval\", or \"os\", got %q", cfg.Storage.SyncPolicy)
	}
	return nil
}
