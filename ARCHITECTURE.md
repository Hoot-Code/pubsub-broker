# Architecture

## Overview

```
                        ┌──────────────────────────────────────────────┐
                        │                  Broker Node A               │
  ┌──────────┐  TCP     │  ┌─────────┐   ┌─────────────────────────┐  │
  │ Producer ├──────────┼─►│ Server  │   │  Topic / PartitionLog   │  │
  └──────────┘  frame   │  │(binary  ├──►│  ┌───────┐  ┌────────┐  │  │
                        │  │protocol)│   │  │  WAL  ├─►│Segment │  │  │
  ┌──────────┐          │  └────┬────┘   │  └───────┘  └───┬────┘  │  │
  │ Consumer │◄─────────┼───────┘        │             ┌───▼────┐  │  │
  │  Group   │  push /  │  ┌────────┐    │             │ Index  │  │  │
  └──────────┘  fetch   │  │  HTTP  │    └─────────────────────────┘  │
                        │  │ Admin  │                                  │
  ┌──────────┐          │  │/metrics│   ┌──────────────────────────┐  │
  │brokectl  ├──────────┼─►│/health │   │      Cluster Module      │  │
  └──────────┘  HTTP    │  └────────┘   │  Bully Election  │  ISR  │  │
                        └──────────────────────┬───────────────────────┘
                                               │  gossip / replication
                        ┌──────────────────────▼───────────────────────┐
                        │                  Broker Node B               │
                        │          (same structure, follower)          │
                        └──────────────────────────────────────────────┘
```

## Storage Engine

### Segment Files

Each partition is stored as a series of fixed-directory segment files. A segment is a flat binary file whose records are appended sequentially. When a segment reaches `segment_max_bytes` a new one is created. Old segments are compacted (deleted) once all their records are older than `max_age_hours` or the total partition size exceeds `max_size_mb`.

### Sparse Index

Each segment has a companion `.idx` file. An index entry is written every `index_interval_bytes` bytes of record data. Each entry stores `(logicalOffset, fileOffset)` so random reads can binary-search to the nearest index point and scan forward, avoiding full-segment linear scans.

### Record Format (V2)

```
┌──────────────┬──────────────┬──────────┬────────────┬─────────┬───────┬────────────────┬──────────────────┐
│  Offset (8B) │Timestamp (8B)│KeyLen (2B)│PayloadLen │  CRC   │ Codec │    Key         │     Payload      │
│   int64 BE   │   int64 BE   │ uint16 BE │  (4B)     │ (4B)   │ (1B)  │  (KeyLen B)    │  (PayloadLen B)  │
│              │              │           │ uint32 BE  │uint32BE│ uint8 │                │                  │
└──────────────┴──────────────┴──────────┴────────────┴─────────┴───────┴────────────────┴──────────────────┘
Total header: 27 bytes. CRC covers everything after itself (Codec + Key + Payload).
```

RecordVersion 1 omits the Codec byte (26-byte header). The version is encoded in the segment filename suffix and stored in a one-byte version preamble at offset 0 of each segment file.

## Understanding Partitions

Messages are routed to partitions deterministically using FNV-1a hashing of the message key. When no key is given, round-robin distribution is used. This means:

- The same key always lands on the same partition (deterministic).
- A message with key `"order-123"` is **not** necessarily in partition 0.
- You do not need to know which partition a message landed in to consume it — `brokectl tail --topic <t>` (no `--partition` flag) scans all partitions.

### Worked Example

Publishing 3 messages with keys `"a"`, `"b"`, `"c"` to a topic with 4 partitions. The partitioner uses `fnv.New32a()` (FNV-1a, 32-bit) and computes `hash(key) % partition_count`:

| Key  | FNV-1a Hash    | Partition (`hash % 4`) |
|------|----------------|------------------------|
| `"a"` | 3,826,002,220 | **0**                  |
| `"b"` | 3,876,335,077 | **1**                  |
| `"c"` | 3,859,557,458 | **2**                  |

All three keys land on different partitions. A user who publishes with key `"b"` and then runs `brokectl tail --topic orders --partition 0` would see **no messages** — not because the message was lost, but because it landed in partition 1. Running `brokectl tail --topic orders` (no `--partition` flag) finds it immediately.

### WAL (Write-Ahead Log)

Before a message is appended to the segment, a WAL entry is fsync'd to `wal_path`. On clean shutdown `Stop()` calls `WAL.Truncate(nil)` to discard all entries (they are now durable in segments). On restart, any remaining WAL entries are replayed into the segment in `Start()`. Replay is idempotent: each WAL entry records a `TargetOffset`; if `pl.NextOffset() > TargetOffset` the entry was already written and is skipped.

## Protocol

### Binary Frame Format

```
┌──────────┬───────────┬───────────┬────────────┬───────────┬──────────────┐
│ Magic(2B)│Version(1B)│Command(1B)│RequestID(4B)│BodyLen(4B)│  Body (NB)  │
│ 0xBE 0xEF│   0x01    │  uint8    │  uint32 BE  │ uint32 BE │ JSON bytes  │
└──────────┴───────────┴───────────┴────────────┴───────────┴──────────────┘
```

Total fixed header: 12 bytes. Body is always UTF-8 JSON. Responses use the same frame format with the same RequestID as the originating request.

### Command Codes

| Code   | Name              | Direction        | Description                                        |
|--------|-------------------|------------------|----------------------------------------------------|
| `0x01` | CmdPing           | client→server    | Liveness check; server replies with CmdPong        |
| `0x02` | CmdPong           | server→client    | Reply to CmdPing                                   |
| `0x03` | CmdAuth           | client→server    | Authenticate with an API key                       |
| `0x04` | CmdAuthOK         | server→client    | Authentication accepted                            |
| `0x05` | CmdPublish        | client→server    | Publish one message; body is PublishRequest        |
| `0x06` | CmdPublishACK     | server→client    | Publish acknowledged; body contains Offset         |
| `0x07` | CmdFetch          | client→server    | Fetch a batch of messages from a partition         |
| `0x08` | CmdFetchResponse  | server→client    | Batch of messages; body is FetchResponse           |
| `0x09` | CmdSubscribe      | client→server    | Enter push-delivery mode for a consumer group      |
| `0x0A` | CmdPush           | server→client    | Server-initiated message delivery (push mode)      |
| `0x0B` | CmdCommit         | client→server    | Commit consumer-group offset                       |
| `0x0C` | CmdCommitACK      | server→client    | Commit acknowledged                                |
| `0x0D` | CmdCreateTopic    | client→server    | Create a topic with partition count and replication|
| `0x0E` | CmdTopicResponse  | server→client    | Topic operation result                             |
| `0x0F` | CmdListTopics     | client→server    | List all topics                                    |
| `0x10` | CmdListConsumers  | client→server    | List consumer groups                               |
| `0x11` | CmdSeekToTime     | client→server    | Seek consumer to first offset ≥ timestamp          |
| `0x12` | CmdSeekResponse   | server→client    | Result of seek; body contains per-partition offsets|
| `0x13` | CmdError          | server→client    | Error response; body contains Code and Message     |
| `0x14` | CmdBatchPublish   | client→server    | Publish multiple messages atomically               |
| `0x15` | CmdBatchACK       | server→client    | Batch publish acknowledged; body contains offsets  |
| `0x16` | CmdDeleteTopic    | client→server    | Delete a topic and its segments                    |
| `0x17` | CmdClusterInfo    | client→server    | Request cluster membership and ISR state           |
| `0x18` | CmdClusterInfoRes | server→client    | Cluster state response                             |

## Clustering

### Consensus Algorithm Selection

As of v0.13, the broker supports two consensus algorithms, selectable via
`consensus_algorithm` in the cluster config:

- **"bully"** (default): Uses the existing Bully election algorithm for
  backward compatibility with existing deployments. Simpler but cannot
  guarantee safety under network partitions.
- **"raft"**: Uses the new Raft implementation which provides split-brain
  safety that Bully cannot guarantee. New deployments are encouraged to use
  Raft. Set `"consensus_algorithm": "raft"` in the cluster config.

`IsLeader()`, `Leader()`, and `Members()` on the cluster Node work
identically regardless of which algorithm is active — callers in broker.go
must not need to know which consensus mechanism is running underneath.

### Bully Election (Legacy)

On startup each node broadcasts an ELECT message to all peers with a higher NodeID. If no higher-ranked peer responds within `election_timeout`, the node declares itself leader and broadcasts a COORDINATOR message. Followers reset their heartbeat timer on each COORDINATOR; if the timer expires they start a new election.

Bully is available for simpler deployments that can tolerate brief split-brain windows; Raft is recommended for strict consistency requirements.

### Raft Consensus

The Raft implementation (`internal/cluster/raft`) follows the original paper
(Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm")
and provides three key safety properties that Bully cannot guarantee:

1. **Election Safety**: At most one leader per term. Enforced via the
   `CurrentTerm`/`VotedFor` persistent check before granting any vote.
   A node only votes once per term, preventing two candidates from both
   winning in the same term.

2. **Log Matching**: If two logs contain an entry with the same index and
   term, the logs are identical in all preceding entries. Enforced via the
   `PrevLogIndex`/`PrevLogTerm` check in AppendEntries — a follower rejects
   any AppendEntries whose preceding entry doesn't match its own log.

3. **Leader Completeness**: If a log entry is committed in a given term,
   it is present in the logs of all leaders for all higher-numbered terms.
   Enforced via the election restriction — a candidate only receives votes
   if its log is at least as up-to-date as the voter's.

These properties are concretely demonstrated by `TestNoSplitVoteSplitBrain`,
which simulates a network partition where nodes {A,B} cannot reach {C,D,E}.
The majority side {C,D,E} elects exactly one leader; the minority side {A,B}
cannot reach quorum and stays leaderless — proving split-brain safety that
Bully cannot guarantee.

Raft state (current term, vote, log) is persisted to disk via a
`FilePersistentStore` using atomic temp-file + rename + fsync, ensuring
that a node that restarts does not forget its vote or committed entries.

### ISR Tracking

The leader maintains an In-Sync Replica (ISR) set. A follower is added to the ISR when it has replicated all messages up to the leader's commit offset. A follower is removed when it falls more than `max_lag` messages behind or misses two consecutive heartbeat intervals. Publish requests with `DeliveryMode=Quorum` block until `⌈ISR/2⌉+1` replicas acknowledge the write.

### Partition Ownership and Rebalancing

The leader assigns each partition to a node using consistent hashing of `(topic, partition)`. When a node joins or leaves, the leader recomputes assignments and broadcasts a REBALANCE message. Followers begin streaming missing segments from the new leader immediately.

## Exactly-Once Delivery

Each `PublishRequest` carries an optional `SeqNum uint64`. The broker maintains a per-`clientID` deduplication ring buffer of the last 1 024 `(clientID, SeqNum)` pairs it has acknowledged. If an incoming publish matches a buffered pair the broker returns the previously assigned offset without re-appending to the log. Clients must increment `SeqNum` monotonically and store it durably before retrying; the broker never deduplicates across restarts (the ring buffer is in-memory only). `DeliveryMode=ExactlyOnce` enforces that `SeqNum > 0`.

## Observability

### Prometheus Metrics

`GET http://<broker>:<admin_port>/metrics` returns metrics in the Prometheus text exposition format. Key metrics:

- `pubsub_published_total{topic}` — cumulative publish count per topic
- `pubsub_fetch_total{topic,group}` — cumulative fetch count
- `pubsub_in_flight_requests` — current concurrency gauge
- `pubsub_segment_bytes{topic,partition}` — on-disk bytes per partition
- `pubsub_replication_lag{topic,partition,node}` — follower lag in messages

### OpenTelemetry Tracing

Every request handler creates a span with the command name as the operation name. Spans are stored in an in-process ring buffer (last 4 096 spans). `GET /traces` returns the buffer as a JSON array compatible with the OpenTelemetry Collector's OTLP/JSON format.

## Delivery Guarantees

| Mode         | Broker guarantee                           | What the client must do                          |
|--------------|--------------------------------------------|--------------------------------------------------|
| AtMostOnce   | Message written to WAL; no retry on error  | Fire and forget; accept possible loss on crash   |
| AtLeastOnce  | Retry until ACK received                   | Deduplicate on consume using message ID          |
| ExactlyOnce  | SeqNum dedup window prevents duplicates    | Store SeqNum durably; always retry with same SeqNum |

## Consumer Group Offset Reset

When a consumer group subscribes in push mode and has never committed an offset (i.e. it is a brand-new group), the broker replays all existing messages from offset 0 before beginning live delivery. This matches Kafka's `auto.offset.reset=earliest` semantics:

- **Earliest (default)**: New consumer groups start from offset 0 and receive all previously published messages.
- **Live delivery**: After the historical replay completes, only messages published after the subscription are delivered via the normal push path.

### Ordering guarantees

The broker sends the Subscribe OK response **before** starting replay, and the client registers its push router **before** the Subscribe round-trip. This eliminates the race where replay CmdPush frames arrive before the client is ready to receive them. Specifically:

1. **Broker side**: `handleSubscribe` sends the OK response, then dispatches replay messages synchronously to the consumer's push channel. The `pushDeliveryLoop` goroutine picks up messages from the channel and writes CmdPush frames. Since OK is sent first, the client always receives the subscription acknowledgment before any replay frames.

2. **Client side**: `Consumer.Subscribe()` registers a push router before calling `sendRecv`. Frames arriving during the round-trip (e.g. from replay) are buffered in an internal buffer and drained to the `Messages()` channel after `sendRecv` returns. This makes the race structurally impossible, not just unlikely.

Groups that have committed offsets resume from the next offset after their last committed position; no replay occurs.

The `brokectl consume` and `brokectl tail` commands both use push delivery. For one-shot, groupless reads at a specific offset, use the `brokectl tail --offset <N>` command or the `Client.Fetch` SDK method, which performs a stateless read without a consumer group.

## Log Compaction

In addition to age/size-based retention (`Compact(maxAgeHours, maxSizeMB)`), a topic can opt into **key-based log compaction** by setting `"compaction_mode": "compact"` in its `TopicConfig`. A compacted topic behaves like a Kafka-style changelog: instead of expiring old messages by age or size, the broker retains only the **latest message per key**, so the topic always reflects current state for every key that has ever been published.

A background `storage.KeyCompactor` sweeps every `compaction.interval_ms` (default 60 s). For each compacted partition it:

1. Scans every segment *except the active one* (the active segment is still being appended to and is never touched).
2. Builds a `key → highest offset` map across those segments, oldest to newest. Messages published with an **empty key are never compacted** — they are always retained, matching Kafka's null-key semantics.
3. Rewrites each segment, keeping only the record at the latest offset for each key (plus all empty-key records), and atomically replaces the on-disk segment file (write to a temp file, `fsync`, `os.Rename`).

### Tombstones

To delete a key from a compacted topic, a producer publishes a **tombstone**: a message for that key with `Headers["_compaction"] = "delete"` (the SDK exposes this as `Producer.Tombstone(ctx, key)`). A tombstone is kept — not removed — until `compaction.tombstone_grace_ms` (default 24 h) has elapsed since it was written. This grace period gives slow consumers time to observe the deletion before the key, and all of its older records, disappear from the log entirely on a later sweep.

### Example: `user-profiles`

A topic named `user-profiles` where each message's key is a user ID acts as a changelog of "current profile per user": publishing with key `"user-42"` and an updated payload replaces the user's previous profile on the next compaction sweep, and publishing a tombstone for `"user-42"` removes the user from the changelog (after the grace period). A consumer that replays the topic from offset 0 after compaction sees exactly one record per user — its latest state — rather than its entire publish history.

## HTTP Gateway

The optional gateway (`internal/gateway`) is a thin HTTP + WebSocket front end for browsers and languages without a native SDK. It is **not** a protocol bypass: internally it is itself a `pkg/client` client of the broker, talking the same binary protocol over loopback TCP as any other SDK consumer. It does not read WAL/segment files directly or duplicate any broker-internal logic.

- **REST** (`/v1/...`): create/list topics, publish a single message or a batch, and fetch messages from a specific partition/offset — all stateless, one-shot operations suited to request/response clients.
- **WebSocket** (`/v1/topics/{topic}/stream`): a long-lived, server-push subscription, internally backed by a `pkg/client.Consumer` in push mode. The WebSocket server itself (`internal/wsutil`) is a minimal, hand-rolled RFC 6455 implementation using only `net`, `crypto/sha1`, `encoding/base64`, and `bufio` from the standard library — the project has zero external dependencies, so no WebSocket library is pulled in. This implementation is shared between the gateway and the broker's embedded admin server (Explorer endpoint).

Use the gateway when a client cannot embed `pkg/client` (a browser, a one-off `curl`/script, a language without a maintained SDK) or when you specifically want a REST/WebSocket surface for ease of integration. Use the native Go or Python SDK directly whenever possible: it talks the binary protocol natively, avoids the extra HTTP/WS hop, and exposes the full feature surface (e.g. quorum delivery modes, exactly-once `SeqNum`) that the gateway's simplified REST shape does not.

The gateway can run two ways: **embedded** in the main broker process (set `"gateway": {"enabled": true, "addr": ":8080"}` in `broker.json`), or as a **separate process** (`cmd/gateway`) pointed at a running broker via `-broker-addr`. Run exactly one of the two against a given broker, not both.

## Live Message Explorer

The Live Message Explorer (`GET /explorer/stream`) provides a WebSocket-based live tail of newly published messages with server-side filtering. It is a **non-consuming live tap** — it does not affect consumer group offsets, DLQ semantics, or offset commits. Messages are observed after they are durably written to the segment, so the Explorer never sees a message that didn't actually get committed.

### Filtering

The client connects with query parameters that define the filter:

- `topic` (required): only messages from this topic.
- `partition` (optional, default all): filter to a specific partition.
- `key` (optional): exact match on the message key.
- `producer` (optional): exact match on the publishing client's `ClientID`, captured live at publish time from `PublishRequest.ClientID`. This is **not** persisted in the segment, so it only works for the live tail — historical replay has no producer identity.
- `search` (optional): case-insensitive substring match against the decoded payload (post-decompression). This is the most expensive filter dimension and is only evaluated after the cheaper topic/partition/key/producer checks pass.

### Backpressure and Drop Policy

Each Explorer session runs its own goroutine with a bounded channel (capacity 256). `Publish()` performs a non-blocking send into each session's channel; if the channel is full, the message is dropped and a drop counter is incremented. The session's goroutine drains the channel and calls the sink (WebSocket write), which can block without affecting the publish hot path.

A status frame `{"status":{"dropped_since_last":N}}` is sent periodically (every 5 seconds) when the drop counter has changed, so the client knows data was lost.

### Pause/Resume

Clients can send `{"action":"pause"}` and `{"action":"resume"}` control frames. When paused, the drain goroutine stops reading from the channel (messages accumulate); on Resume, the drain catches up from whatever is buffered. A brief pause does not lose the most recent N messages.

### Connection Cap

`ExplorerMaxConnections` (default 50) caps the number of concurrent Explorer WebSocket connections. New upgrades beyond this limit receive HTTP 503.

### Metrics

- `explorer_active_sessions` — current number of active Explorer sessions.
- `explorer_messages_sent_total` — cumulative messages successfully delivered.
- `explorer_messages_dropped_total` — cumulative messages dropped due to slow consumers.

## Dashboard

The embedded Operational Control Center (`GET /dashboard`) is a multi-file single-page application (ES modules, zero build step) served via `go:embed`. The directory structure is `internal/broker/assets/dashboard/` with `index.html`, `style.css`, and per-section JS modules. A `FileServerFS` with `http.StripPrefix` serves static assets under `/dashboard/`.

### Sections and Backend Endpoints

| Section | Endpoints Used |
|---|---|
| **Overview** | `/topics`, `/consumers`, `/cluster/members`, `/cluster/raft`, `/healthz/ready` |
| **Topics** | `/topics`, `/consumers` |
| **Partitions** | `/topics`, `/topics/{topic}/partitions` |
| **Consumer Groups** | `/consumers`, `/consumers/{group}/{topic}` |
| **Live Explorer** | `/explorer/stream` (WebSocket) |
| **DLQ** | `/dlq`, `/dlq/replay`, `/dlq/{id}`, `/dlq/{id}/export` |
| **Cluster** | `/cluster/members`, `/cluster/raft`, `/cluster/isr` |
| **Metrics** | `/metrics/history?range=5m\|15m\|1h\|24h` |
| **Audit Logs** | `/audit/recent` |
| **Settings** | `/config/effective` (admin-only, read-only) |

### RBAC and Auth

Authentication uses the same API-key/session-cookie system as the binary protocol. The dashboard session endpoint (`GET /dashboard/session`) returns the current identity (client_id, role, topic allowlist). Client-side RBAC gating hides write actions for viewers; all security enforcement is server-side via `identity.Can()` and the `requireAuth` middleware.

### Write Actions

DLQ replay (`POST /dlq/replay`) and DLQ delete (`DELETE /dlq/{id}`) are the only write actions, both gated to admin role in the UI. Topic creation is binary-protocol-only. Settings are read-only.

The dashboard is gated by `dashboard_enabled` in the network config (default: true); when false, `GET /dashboard` returns 403 Forbidden.

## Configuration Management

### Hot-Reload Whitelist

The broker maintains a strict whitelist of config fields that may be changed on a running instance without a restart. All other settings (network host/port, TLS cert/key paths, cluster consensus algorithm, storage paths, auth API keys, etc.) are intentionally **not** hot-reloadable because changing them at runtime would be unsafe, require re-establishing connections, or have no meaningful effect.

**Hot-reloadable fields** (defined in `internal/config/hotreload.go`):
- `rate_limit.enabled`, `rate_limit.per_client_rps`, `rate_limit.per_topic_rps`
- `retention.max_age_hours`, `retention.max_size_mb`
- `compaction.interval_ms`, `compaction.tombstone_grace_ms`
- `drain_timeout_ms`, `flow_control_pause_ms`
- `logging.level`

### Safety Model

The PATCH /config endpoint enforces a **validate-before-apply** safety model:

1. **Whitelist check**: Every field path in the request is checked against `HotReloadableFields`. Non-hot-reloadable fields are rejected with `rejected_fields` listing ALL rejected paths in one response.
2. **Type validation**: Each field value is validated for correct type and range (e.g. `per_client_rps > 0`, `logging.level` is one of the four valid levels).
3. **Full config validation**: The candidate config (with patch applied) is validated through the same `config.Validate()` function used at startup.
4. **Apply to live components**: Only after all validation passes, the patch is applied to running subsystems (rate limiter, retention loop, compactor, logger, atomic config values).
5. **Persist to disk**: The full updated config is written to the original config file using atomic temp-file + rename.
6. **Audit log**: Every change is logged with old/new values for each field.

A rejected patch (at any stage) leaves the broker in exactly its prior state — no live state or disk file is touched.

### Disk Persistence

PATCH /config writes the FULL config (not just the patch) back to the original file. The file uses `json.MarshalIndent` for human readability and preserves all `_comment_*` documentation fields present in the committed `configs/broker.json` (comments survive round-trips through JSON marshal/unmarshal because they are valid JSON keys).
