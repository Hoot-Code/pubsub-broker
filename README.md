# pubsub-broker

[![Build and Test](https://github.com/Hoot-Code/pubsub-broker/actions/workflows/ci.yml/badge.svg)](https://github.com/Hoot-Code/pubsub-broker/actions/workflows/ci.yml) [![GitHub](https://img.shields.io/badge/GitHub-Hoot--Code/pubsub--broker-blue)](https://github.com/Hoot-Code/pubsub-broker) | [فارسی](README_fa.md)

A production-ready, self-contained pub/sub message broker written in pure Go with zero external dependencies. It provides partitioned topics, exactly-once delivery, built-in multi-node clustering, Prometheus-compatible metrics, and OpenTelemetry-compatible tracing — all from a single statically-compiled binary that you can drop into any environment without a runtime.

## Features

- **Partitioned topics** — messages distributed deterministically (FNV-1a key hash) or round-robin across configurable partition counts
- **Consumer groups** — multiple independent consumers track committed offsets per partition; supports seek-to-offset and seek-to-timestamp
- **Push delivery** — server-initiated `CmdPush` frames eliminate polling; clients subscribe once and receive messages as they arrive
- **WAL-backed durability** — every publish is written to a Write-Ahead Log before the segment; the broker survives OS crashes without message loss
- **ISR replication** — In-Sync Replica tracking with quorum writes; a follower falling behind is removed from the ISR set automatically
- **Bully leader election** — single-round leader election with heartbeat-based failure detection (see ARCHITECTURE.md for limitations)
- **TLS** — TLS 1.3 on both the binary protocol port and the HTTP admin port
- **Message compression** — per-message flate/zlib codec negotiated at publish time, transparent to consumers
- **Seek-to-timestamp** — binary-search scan finds the first offset whose record timestamp ≥ a given nanosecond value
- **Graceful drain** — `Stop()` waits for all in-flight requests to complete before tearing down connections

## Comparison

| Feature                | pubsub-broker  | NSQ        | NATS core         |
|------------------------|----------------|------------|-------------------|
| Persistence            | ✅ WAL + segment | ✅ disk    | ❌ memory only    |
| Partitions             | ✅              | ❌         | ❌ (JetStream only)|
| Consumer groups        | ✅              | ✅ channel | ❌                |
| Clustering             | ✅ Bully + ISR  | ✅         | ✅                |
| Exactly-once delivery  | ✅ SeqNum dedup | ❌         | ❌                |
| Push delivery          | ✅              | ✅         | ✅                |
| Compression            | ✅ flate / zlib | ✅         | ✅                |
| Zero-dependency binary | ✅              | ✅         | ✅                |

## Quickstart (Docker)

> **Note on partitions:** Messages are routed to partitions by hashing the
> message key (or round-robin if no key is given). A message with key
> `"order-1"` will consistently land in the same partition every time, but
> that partition is **not** necessarily 0. Use `brokectl tail --topic <t>`
> (no `--partition` flag) to scan all partitions — don't assume partition 0.

```bash
docker-compose up -d
brokectl --addr 127.0.0.1:9000 topic create --name orders --partitions 4
brokectl --addr 127.0.0.1:9000 publish --topic orders --key order-1 --payload '{"id":1,"amount":99.00}'
brokectl --addr 127.0.0.1:9000 consumer list
brokectl --addr 127.0.0.1:9000 tail --topic orders --count 5
brokectl --addr 127.0.0.1:9000 health
```

`tail` scans all partitions by default, since messages are distributed by key hash — you don't need to know which partition a message landed in to find it.

## One-Click Install

For a quick start without Docker, run the quickstart script:

```bash
curl -fsSL https://raw.githubusercontent.com/Hoot-Code/pubsub-broker/main/quickstart.sh | bash
```

Or if you've cloned the repo:

```bash
chmod +x quickstart.sh && ./quickstart.sh
```

This builds the broker and brokectl, creates a sample topic, and publishes 5 messages. Press Ctrl-C to stop the broker.

The quickstart includes an interactive authentication wizard that lets you:
- **Automatic** — generate a secure API key automatically (recommended)
- **Manual** — enter your own API key (min 32 characters)
- **Disable** — skip authentication for development only

## Quickstart (Go SDK)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/Hoot-Code/pubsub-broker/pkg/client"
)

func main() {
    // Dial the broker — no external dependencies required.
    c, err := client.Dial("127.0.0.1:9000",
        client.WithDialTimeout(10*time.Second),
        client.WithReadTimeout(30*time.Second),
    )
    if err != nil {
        log.Fatalf("dial: %v", err)
    }
    defer c.Close()

    // Authenticate (omit if auth is disabled in broker config).
    if err := c.Authenticate("my-api-key"); err != nil {
        log.Fatalf("auth: %v", err)
    }

    ctx := context.Background()

    // Publish a message and receive its assigned offset.
    prod := c.NewProducer("orders")
    offset, err := prod.Publish(ctx, "key-1", []byte(`{"amount":99}`), nil)
    if err != nil {
        log.Fatalf("publish: %v", err)
    }
    fmt.Printf("published at offset %d\n", offset)

    // Create a consumer in a named group and subscribe for push-based delivery.
    // Messages are distributed across partitions by key hash, so we don't
    // specify a partition — the consumer group receives from all partitions.
    cons := c.NewConsumer("my-group", "orders")
    if err := cons.Subscribe(ctx); err != nil {
        log.Fatalf("subscribe: %v", err)
    }
    for msg := range cons.Messages() {
        fmt.Printf("partition=%d offset=%d payload=%s\n", msg.Partition, msg.Offset, msg.Payload)
        // Commit offset so the group advances past this message.
        _ = cons.Commit(ctx, msg.Partition, msg.Offset)
    }
}
```

## Quickstart (HTTP Gateway)

For browsers or languages without a native SDK, enable the optional HTTP/WebSocket gateway (`"gateway": {"enabled": true, "addr": ":8080"}` in `broker.json`, or run `go run ./cmd/gateway -broker-addr 127.0.0.1:9000 -addr :8080` as a separate process) and use plain `curl`:

```bash
# Create a topic
curl -s -X POST http://127.0.0.1:8080/v1/topics \
     -d '{"name":"orders","partitions":4}'

# Publish a message
curl -s -X POST http://127.0.0.1:8080/v1/topics/orders/messages \
     -d '{"key":"order-1","payload":"hello"}'

# Fetch from a specific partition (REST API is partition-specific —
# the key "order-1" hashes to a particular partition, not necessarily 0.
# Use brokectl tail --topic orders (no --partition flag) to scan all.)
curl -s 'http://127.0.0.1:8080/v1/topics/orders/partitions/0/messages?offset=0&limit=10'
```

To subscribe over WebSocket, the project has zero dependencies so there's no bundled JS/Python WS client — use any RFC 6455-compliant tool, e.g. [`websocat`](https://github.com/vi/websocat):

```bash
websocat "ws://127.0.0.1:8080/v1/topics/orders/stream?group=my-group&consumer=c1"
```

...or this minimal, dependency-free Python snippet using only the standard library `socket`/`hashlib`/`base64` (no `websockets` package required, consistent with the project's zero-dependency policy):

```python
import socket, base64, hashlib, os

key = base64.b64encode(os.urandom(16)).decode()
sock = socket.create_connection(("127.0.0.1", 8080))
sock.send((
    "GET /v1/topics/orders/stream?group=my-group HTTP/1.1\r\n"
    "Host: 127.0.0.1:8080\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
    f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
).encode())
print(sock.recv(4096))  # 101 Switching Protocols + first frames arrive here
```

## Configuration

Generate a default configuration file with the bundled tool:

```bash
go run ./cmd/gen-config > broker.json
```

Example output (abbreviated):

```json
{
  "broker":  { "node_id": "node-a" },
  "network": { "host": "0.0.0.0", "port": 9000, "max_connections": 10000,
               "read_timeout": 30000000000, "write_timeout": 30000000000 },
  "storage": { "data_path": "./data", "wal_path": "./data/wal",
               "segment_max_bytes": 134217728, "sync_policy": "always" },
  "auth":    { "enabled": false }
}
```

For Kubernetes deployment see `deploy/k8s/` — includes a StatefulSet, ConfigMap, Service, and PodDisruptionBudget.

## Benchmarks

Measured on Linux, AMD64, Intel Core i7-12700K, Go 1.22. Numbers from `go test -bench=. -benchtime=5s ./tests/benchmarks/`.

| Benchmark                 | ops/sec      | MB/s | Latency p99 |
|---------------------------|--------------|------|-------------|
| Publish (1 KB payload)    | 220,000      | 220  | 0.6 ms      |
| Publish (16 KB payload)   |  50,000      | 800  | 1.2 ms      |
| Fetch (100-msg batch)     | 180,000 msg/s| 180  | 0.8 ms      |
| Publish ExactlyOnce       | 110,000      | 110  | 0.9 ms      |
| Batch Publish (50 msg)    | 400,000 msg/s| 400  | 1.5 ms      |

See `tests/benchmarks/README.md` for full methodology and variance data.

## Architecture

The broker wires a binary TCP server, an append-only segment log, a Write-Ahead Log, consumer-group offset tracking, optional cluster membership, and an HTTP admin server into a single `Broker` orchestrator. See [ARCHITECTURE.md](ARCHITECTURE.md) for a complete diagram, protocol command reference, and storage engine deep-dive.

## Dashboard

The broker includes an embedded Operational Control Center accessible at `GET /dashboard` (or `GET /` which redirects there). The dashboard is a multi-file single-page application (ES modules, zero build step) embedded via `go:embed` with a dark theme and system font stacks. No external CDN resources are loaded.

### Sections

- **Overview** — topic/partition counts, active connections, consumer groups, cluster status, health badge, uptime
- **Topics** — topic list with partition count, message count, storage size, retention policy, and consumer group count
- **Partitions** — per-partition detail (leader, replicas, ISR, WAL status, under-replicated badge, segment info)
- **Consumer Groups** — expandable group+topic pairs showing members, rebalancing status, per-partition committed/current offset and lag
- **Live Explorer** — WebSocket-based live message tail with topic/partition/key/producer/payload filters, pause/resume, 500-message DOM cap
- **DLQ** — dead-letter queue browser with per-entry replay, delete, export, and bulk purge (admin-only actions)
- **Cluster** — node cards, leader/follower visualization, Raft internals (term, commit index, peer match/next index), ISR state table
- **Metrics** — time-range charts (5m/15m/1h/24h) for publish/consume rate, connections, memory, CPU, WAL throughput, consumer lag
- **Audit Logs** — last 100 events with client-side search/filter by client, type, or topic
- **Settings** — read-only display of effective configuration (editing support planned for a future phase)

The dashboard requires authentication when `auth.enabled` is true (configurable via `network.dashboard_auth_enabled`). RBAC gating is enforced client-side for UX; all security is enforced server-side.

**Authentication flow:**
- Unauthenticated users see the login page at `/dashboard`
- Session cookies are `HttpOnly`, `SameSite=Strict`, and expire after 12 hours (configurable via `network.dashboard_session_ttl`)
- Logout clears the session server-side and the cookie client-side
- Expired sessions automatically redirect to the login page

![Dashboard](docs/dashboard-screenshot.png)

## License

MIT — see `LICENSE`.
