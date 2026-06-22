# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Settings write-path: PATCH /config for a carefully whitelisted set of
  hot-reloadable fields (rate limits, retention, compaction, drain/flow-control
  timing, log level), with disk persistence, full validation before applying,
  and audit logging. All other settings remain read-only and require a restart
  by design.

### Changed
- Full rewrite of the embedded dashboard UI: replaced single-file `dashboard.html`
  with a multi-file Operational Control Center (`internal/broker/assets/dashboard/`)
  using ES modules (zero build step). New sections: Overview, Topics, Partitions,
  Consumer Groups, Live Explorer (WebSocket), DLQ, Cluster, Metrics (SVG charts),
  Audit Logs, Settings. Hand-rolled SVG line-chart helper (`charts.js`) reused by
  Metrics — no external charting library. Settings is read-only.
- `GET /topics` response now includes `StorageSizeBytes` (sum of segment sizes
  across partitions) for the Topics table.
- New `GET /config/effective` endpoint (admin-only, read-only) returns the
  broker's current effective configuration with API key values redacted.
- Dashboard static assets (CSS, JS) served via `http.FileServerFS` mounted at
  `/dashboard/` with `http.StripPrefix`, replacing the single embedded HTML blob.
- `go:embed` directive updated from `assets/dashboard.html` to `assets/dashboard/*`
  (directory embed).

### Added
- Dashboard login (session-cookie based, reusing the existing API-key/RBAC
  identity system) gating the embedded dashboard and all dashboard-backing
  JSON endpoints. Deliberate decision NOT to build a separate user database:
  login credentials are the existing broker API keys. The auth.Enabled=false
  escape hatch disables dashboard auth for trusted-network deployments.
  SameSite=Strict cookie is the primary CSRF mitigation for write actions.
- POST /dashboard/login: authenticates via API key, sets HttpOnly session
  cookie, rate-limited by remote IP (default 10 req/min).
- POST /dashboard/logout: revokes session and clears cookie.
- GET /dashboard/session: bootstrap endpoint returning current identity.
- SessionStore: in-memory, thread-safe session store with configurable TTL
  (default 12h), lazy eviction on read, and periodic cleanup goroutine.
- Unified identity resolution middleware: Authorization header (for brokectl/
  API callers) and dashboard session cookie (for browser) both resolve to the
  same Identity, feeding existing permission checks unchanged.
- Dashboard auth auto-detection: auth is required when both DashboardEnabled
  and auth.Enabled are true; explicit override to force OFF for trusted
  networks. /metrics and /healthz/* never require auth (Prometheus/k8s
  compatibility preserved).
- Dashboard login page (login.html): minimal, dependency-free, dark-theme
  form for API key entry, served when auth is required.

### Added
- Live Message Explorer backend: GET /explorer/stream WebSocket endpoint
  with server-side topic/partition/key/producer/payload filtering, pause/resume,
  and bounded backpressure (drop-oldest with a reported drop counter) so a slow
  viewer never affects publish throughput.
- internal/wsutil: extracted the hand-rolled RFC 6455 WebSocket implementation
  into a shared, dependency-free package used by both the optional HTTP gateway
  and the embedded admin server.

### Fixed
- Critical: race condition where new-consumer-group replay CmdPush frames
  arrived before the client registered its push router, silently dropping
  all replayed messages. The broker now sends the Subscribe OK response
  BEFORE starting replay, and the client registers its push router BEFORE
  the Subscribe round-trip, eliminating the race structurally.
- Flaky integration tests caused by ephemeral port TOCTOU race in
  freePort() helper: tests now use port 0 so the OS assigns the port
  atomically at bind time with no intermediate listen/close step.
- Critical: configs/broker.json had network.port=9001 (should be 9000),
  causing the documented Docker quickstart to expose the wrong internal
  ports — HTTP admin/dashboard landed on container:9002 instead of
  container:9001, making the Docker quickstart non-functional.
- Config drift root cause: cmd/gen-config and internal/config each
  defined default port independently; now a single config.GenerateDefault()
  function is the source of truth, with TestCommittedConfigMatchesGenConfig
  enforcing sync in every CI pass.
- Raft follower advanced commitIndex on a failed AppendEntries
  consistency check, risking State Machine Safety violations under log
  divergence. The commitIndex advance block is now gated behind a
  successful consistency check (PrevLogIndex/PrevLogTerm match).

### Added
- E2E regression test (TestNewConsumerGroupReplayE2E) using the actual
  pkg/client SDK code path (Dial, NewConsumer, Subscribe, Messages) against
  a real broker, validating that new-consumer-group replay works end-to-end.
- Race-loop stress test (TestNewConsumerGroupReplayRaceLoop) that runs the
  E2E replay test 10 iterations under -race to prove structural correctness.
- Runtime "broker ports bound" log line in broker.Start() that emits both
  tcp_addr and http_admin_addr together, making port misconfiguration
  immediately diagnosable from container logs alone.
- Docker smoke test now verifies TCP binary protocol ping/pong in addition
  to HTTP /health, and includes TestDockerSmokeRejectsPortDrift to prove
  the test can catch wrong-port configs.
- Embedded single-page dashboard at GET /dashboard (dark theme,
  zero external dependencies, polls existing JSON/Prometheus endpoints
  client-side). DashboardEnabled config option (default: true).

### Added
- Raft consensus algorithm as an alternative to Bully
  (consensus_algorithm: "raft" in cluster config). Provides split-brain
  safety that Bully cannot guarantee. Bully remains the default for backward
  compatibility; new deployments are encouraged to use Raft.
- Gateway connection pool keyed by API key — each distinct key gets its own
  dedicated, already-authenticated connection, fixing the RBAC bypass under
  concurrent requests.
- Key-based log compaction: TopicConfig.CompactionMode="compact" retains only the latest message per key per topic; age/size retention is skipped for compacted topics.
- Tombstone support for compacted topics: Producer.Tombstone(ctx, key) marks a key deleted; configurable TombstoneGraceMs delays purge so slow consumers can observe the deletion.
- storage.KeyCompactor: background sweep loop that rewrites non-active segments in place (temp file + fsync + atomic rename), never touching the active segment.
- HTTP/WebSocket gateway: REST endpoints under /v1/ for topic management, publish (single + batch), and fetch; a WebSocket streaming endpoint for server-push subscriptions.
- Minimal, stdlib-only RFC 6455 WebSocket server (internal/gateway/websocket.go) — zero external dependencies, consistent with the project's dependency policy.
- cmd/gateway: standalone gateway binary; alternatively the gateway can run embedded in the broker process via gateway.enabled in config.
- pkg/client.Client.Fetch: stateless, direct-offset read (no consumer group) used by the HTTP gateway's REST fetch endpoint.
- brokectl tail: live message streaming with --follow mode and offset seek.
- DLQ HTTP API: list, replay, and purge dead-letter entries via /dlq endpoints.
- pprof profiling endpoint (opt-in via pprof_enabled config) under /debug/pprof/.
- brokectl consume: single-batch interactive fetch with auto-commit.
- brokectl top: live metrics dashboard with refreshable summary.
- brokectl dlq: CLI for DLQ list, replay, and purge operations.
- brokectl pprof: CLI for downloading cpu, heap, goroutine, and trace profiles.
- SeekToOffset: consumer offset seeking by absolute offset (pkg/client).
- Instance-level push routing in client library.
- Per-consumer push routing keyed by topic, group, and consumerID.
- PushActive counter to manage read-deadline for idle push connections.
- Broker WaitGroup to track offset-checkpoint goroutine lifecycle.
- Per-partition mutex to serialise TargetOffset capture and WAL Append.
- Nack requeue routing to originating group only via DispatchToGroup.
- Slow consumer retry semantics with configurable maxRetries.
- Consumer group validation to reject names containing "/".
- HTTP admin port overflow guard when TCP port is 65535.
- Config validation for network.port range.
- Graceful drain with in-flight request wait before shutdown.
- Hot-reload for auth and rate-limiter with atomic updates.
- Audit logger with safe nil-check before writing events.
- Topic metadata WAL for persistence across restarts.
- Partition ownership tracking for ISR and cluster sync.
- Metrics registry with thread-safe concurrent exposure.
- SpanStore bounded to 1000 entries to prevent unbounded memory growth.
- Cluster node graceful stop with error logging.
- Prometheus metric HELP string escape for backslash and newline.
- Label value escape for backslash, double-quote, and newline.
- Client readLoop to clear deadline when push consumers are active.
- Client handshake using dialTimeout instead of readTimeout.
- Encoder to flush write buffer before blocking on response.
- Batch publish to return offsets in correct order.
- ExactlyOnce producer to auto-increment SeqNum.
- Error response to map broker error codes to typed client errors.
- Consumer push handler to filter by topic, group, and consumerID.
- Partition log NotifyAppend to avoid busy-polling in PollPartitionLog.
- Config hot-reload to fire registered callbacks on file change.
- WAL truncation after replay to prevent duplicate messages on restart.
- Leader election metrics tracked via gauges and counters.
- ISR size averaged across all registered partitions.
- Under-replicated partition count tracked against MinISR threshold.
- Consumer lag total computed from committed offsets across all groups.
- Offset checkpoint to fire on commit count threshold (every 1000 commits).

### Fixed
- Critical: HTTP gateway shared a single authenticated connection across
  concurrent requests with different API keys, allowing RBAC bypass under
  request timing.
- Segment rewrite during compaction left file descriptors closed on a
  partial rename failure (index file rename error after log file rename
  succeeded). reopenOriginal now always reopens the segment before returning.

## [0.11.0] — Security + Refactor
### Added
- API key authentication with RBAC roles (admin, producer, consumer, viewer).
- Topic-level ACL enforcement for publish, subscribe, and seek operations.
- Per-client and per-topic token-bucket rate limiting with configurable burst.
- TLS support for TCP and HTTP servers (mutual TLS for cluster).
- Structured audit logging with append-only event file.
- Kubernetes health probes: /healthz/live, /healthz/ready, /healthz/startup, /healthz/drain.
- Connection draining with configurable timeout before shutdown.
- Hot-reload configuration via file polling (5 s interval).

## [0.10.0] — Ecosystem (Python SDK, docs)
### Added
- Python client SDK (python-client/) with async support.
- Protocol documentation (PROTOCOL.md).
- Architecture documentation (ARCHITECTURE.md).
- Contributing guidelines (CONTRIBUTING.md).

## [0.9.0] — Cluster
### Added
- Raft-based leader election with configurable timeouts.
- Member discovery via seed nodes.
- Partition ownership map with ISR tracking.
- Pull-based follower replication.
- Cluster metrics: election count, leader changes, current term.

## [0.8.0] — Observability
### Added
- Prometheus-format metrics (counters, gauges, histograms).
- Request tracing with span store (last 1000 spans).
- Cluster ISR metrics and replication lag tracking.

## [0.7.0] — Consumer Groups
### Added
- Consumer group membership with auto-rebalancing.
- Persistent offset tracking via dedicated WAL.
- Consumer seek by timestamp, offset, or end.
- Consumer group reset to beginning.
- Dead Letter Queue with bounded size.

## [0.6.0] — Delivery Guarantees
### Added
- At-most-once, at-least-once, and exactly-once delivery modes.
- Exponential backoff retry on broker overload.
- Nack with automatic requeue to originating group.
- Ack/Nack protocol frames.

## [0.5.0] — Storage
### Added
- Segment-based storage engine with sparse offset index.
- Write-Ahead Log with CRC32 checksums and crash recovery.
- Segment roll-over and retention policies.
- Compression support (flate, zlib).

## [0.4.0] — Networking
### Added
- Binary TCP protocol (PSB v1) with framing and CRC validation.
- Request correlation with 64-bit request IDs.
- Push delivery mode for server-initiated message streaming.
- Raw transfer (sendfile) fast-path for large messages.

## [0.3.0] — Routing
### Added
- Hash-based partition routing by message key.
- Round-robin partition routing.
- Topic-level partition configuration.

## [0.2.0] — Core
### Added
- Topic create/delete/list operations.
- Message publish with partition assignment.
- Message fetch from partition log.
- Consumer subscribe/unsubscribe.

## [0.1.0] — Initial
### Added
- Initial project structure and build infrastructure.
- GitHub Actions CI pipeline.
- Benchmark suite.
- Docker and Kubernetes deployment manifests.
