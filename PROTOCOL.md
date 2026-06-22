# Wire Protocol Specification

Version: **1**  
Transport: **TCP** (persistent connections)  
Byte order: **Little-endian**

---

## Frame Format

Every request and response is wrapped in a fixed-size header followed by a
variable-length JSON body.

```
 0               1               2               3
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
├───────────────────────────────────────────────────────────────────┤
│                     Magic (4 bytes = 0x50534201)                  │
├───────────────┬───────────────────────────────────────────────────┤
│  Version (1B) │               Command (1 byte)                    │
├───────────────────────────────────────────────────────────────────┤
│                     Request ID (8 bytes)                          │
│                                                                   │
├───────────────────────────────────────────────────────────────────┤
│                     Body Length (4 bytes)                         │
├───────────────────────────────────────────────────────────────────┤
│                     Body (Body Length bytes)                      │
│                       (JSON encoded)                              │
└───────────────────────────────────────────────────────────────────┘
```

| Field | Size | Description |
|---|---|---|
| Magic | 4 B | `0x50534201` ("PSB\x01") — protocol identifier |
| Version | 1 B | Protocol version. Currently `1`. |
| Command | 1 B | Operation type (see table below) |
| Request ID | 8 B | Client-assigned correlation ID. Echoed in response. |
| Body Length | 4 B | Byte length of the body. `0` for frames with no body. Max `16 MiB`. |
| Body | N B | UTF-8 JSON payload. Schema depends on Command. |

Total header size: **18 bytes**.

---

## Command Reference

| Hex | Name | Direction | Body Schema |
|---|---|---|---|
| `0x01` | `PUBLISH` | C→S | `PublishRequest` |
| `0x02` | `SUBSCRIBE` | C→S | `SubscribeRequest` |
| `0x03` | `UNSUBSCRIBE` | C→S | `SubscribeRequest` |
| `0x04` | `FETCH` | C→S | `FetchRequest` |
| `0x05` | `ACK` | C→S | `AckRequest` |
| `0x06` | `NACK` | C→S | `AckRequest` |
| `0x07` | `COMMIT_OFFSET` | C→S | `CommitOffsetRequest` |
| `0x08` | `CREATE_TOPIC` | C→S | `CreateTopicRequest` |
| `0x09` | `DELETE_TOPIC` | C→S | `DeleteTopicRequest` |
| `0x0A` | `LIST_TOPICS` | C→S | *(empty)* |
| `0x0B` | `AUTH` | C→S | `AuthRequest` |
| `0x0C` | `PING` | C→S | *(empty)* |
| `0x0D` | `PONG` | S→C | *(empty)* |
| `0x0E` | `RESPONSE` | S→C | command-specific |
| `0x0F` | `ERROR` | S→C | `ErrorResponse` |

---

## Body Schemas

### `AuthRequest`
```json
{ "api_key": "string", "client_id": "string" }
```

### `AuthResponse`
```json
{ "ok": true, "permissions": ["publish","subscribe"] }
```

### `PublishRequest`
```json
{
  "topic":         "orders",
  "key":           "customer-42",
  "payload":       "<base64-encoded bytes>",
  "headers":       { "trace-id": "abc" },
  "delivery_mode": 1
}
```
`delivery_mode`: `0` = at-most-once, `1` = at-least-once

### `PublishResponse`
```json
{ "message_id": "uuid", "partition": 2, "offset": 1042 }
```

### `SubscribeRequest`
```json
{ "topic": "orders", "group": "payment-svc", "consumer_id": "worker-1", "push": true }
```

When `push` is `true`, the broker enters push-delivery mode: server-initiated
`CmdPush` frames deliver messages as they arrive. For a brand-new consumer group
(no previously committed offset), the broker first replays all existing messages
from offset 0 on each partition (auto.offset.reset=earliest), then switches to
live delivery. Groups with committed offsets resume from the next offset after
the last committed position.

When `push` is `false` (or omitted), the client uses pull-mode `CmdFetch` to
retrieve messages at a specific offset. Pull-mode fetches are stateless and
do not replay historical messages automatically; the client specifies the
starting offset directly.

### `FetchRequest`
```json
{
  "topic":     "orders",
  "group":     "payment-svc",
  "partition": 2,
  "offset":    1042,
  "max_count": 100
}
```

### `FetchResponse`
```json
{
  "topic":     "orders",
  "partition": 2,
  "messages":  [ { "id": "...", "offset": 1042, "payload": "...", ... } ]
}
```

### `CommitOffsetRequest`
```json
{
  "group":       "payment-svc",
  "consumer_id": "worker-1",
  "topic":       "orders",
  "partition":   2,
  "offset":      1042
}
```

### `CreateTopicRequest`
```json
{
  "name":               "orders",
  "partitions":         4,
  "replication_factor": 3,
  "retention_hours":    24
}
```

### `DeleteTopicRequest`
```json
{ "name": "orders" }
```

### `OKResponse`
```json
{ "ok": true }
```

### `ErrorResponse`
```json
{ "code": "TOPIC_NOT_FOUND", "message": "topic \"orders\" not found" }
```

---

## Error Codes

| Code | Meaning |
|---|---|
| `TOPIC_NOT_FOUND` | Topic does not exist |
| `TOPIC_EXISTS` | Topic already exists |
| `UNAUTHORIZED` | Missing or invalid credentials |
| `INVALID_MESSAGE` | Malformed message body |
| `PARTITION_NOT_FOUND` | Partition index out of range |
| `BROKER_OVERLOADED` | Rate limit exceeded |
| `INTERNAL_ERROR` | Unexpected server error |
| `RETRY_EXCEEDED` | Message moved to DLQ after max retries |
| `BAD_REQUEST` | Malformed frame or missing required fields |
| `UNKNOWN_COMMAND` | Unrecognised command byte |

---

## Session Sequence

### Authenticated Publish

```
Client                                Server
  │                                     │
  │── AUTH { api_key: "..." } ─────────→│ validate key
  │←─ RESPONSE { ok: true, perms: [...] }│
  │                                     │
  │── CREATE_TOPIC { name: "orders" } ──→│
  │←─ RESPONSE { ok: true }             │
  │                                     │
  │── PUBLISH { topic: "orders", ... } ─→│ write to WAL + segment
  │←─ RESPONSE { offset: 0, part: 2 }   │
  │                                     │
  │── PUBLISH { ... }                   │
  │←─ RESPONSE { offset: 1, ... }       │
```

### Consumer Fetch Loop

```
Client (consumer group)              Server
  │                                     │
  │── SUBSCRIBE { topic, group, id } ──→│ join group, get partition assignment
  │←─ RESPONSE { ok: true }             │
  │                                     │
  │── FETCH { partition: 0, offset: 0 }→│ read from storage
  │←─ RESPONSE { messages: [...] }      │
  │                                     │
  │── COMMIT_OFFSET { offset: 4 } ─────→│ persist offset
  │←─ RESPONSE { ok: true }             │
  │                                     │
  │── FETCH { partition: 0, offset: 5 }→│ (long-polls until messages arrive)
  │←─ RESPONSE { messages: [...] }      │
```

### Keepalive

```
Client          Server
  │── PING ────→│
  │←── PONG ────│
```

Clients should send a PING every 30 s on idle connections. The server closes
connections idle for longer than `network.idle_timeout`.

---

## Future Versions

Version 2 will replace the JSON body with a length-prefixed binary encoding
(Protocol Buffers) for lower serialisation overhead. The header magic and
command table will remain backward compatible.
