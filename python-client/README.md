# pubsub-broker Python Client

Pure Python client for [pubsub-broker](../README.md). No external dependencies — stdlib only (`socket`, `threading`, `struct`, `json`, `base64`). Supports Python 3.9+.

## Installation

No `pip` step needed. Copy the `pubsub/` directory next to your code:

```bash
cp -r python-client/pubsub ./pubsub
```

Or add the `python-client/` path to `PYTHONPATH`:

```bash
export PYTHONPATH=/path/to/repo/python-client:$PYTHONPATH
```

## Quickstart

```python
from pubsub import Client

# Connect (Ping/Pong handshake happens automatically)
with Client("127.0.0.1:9000", api_key="my-secret") as c:

    # Publish a message
    prod = c.new_producer("orders")
    offset = prod.publish("order-1", b'{"amount": 99}')
    print(f"published at offset {offset}")

    # Subscribe for push-based delivery (receives from all partitions).
    # Messages are routed by key hash, so don't assume partition 0.
    cons = c.new_consumer("my-group", "orders")
    cons.subscribe()
    for msg in cons.messages():
        print(f"partition={msg.partition} offset={msg.offset} payload={msg.payload}")
        cons.commit(msg.partition, msg.offset)
```

## API Reference

### `Client(addr, *, api_key="", tls=False, dial_timeout=10.0, read_timeout=30.0)`

Connects to the broker at `host:port`. Performs the Ping/Pong handshake on init; sends `CmdAuth` if `api_key` is non-empty. Use as a context manager (`with Client(...) as c`) for automatic cleanup.

| Method | Description |
|---|---|
| `c.new_producer(topic)` | Create a `Producer` for `topic` |
| `c.new_consumer(group, topic)` | Create a `Consumer` in `group` for `topic` |
| `c.close()` | Close the TCP connection |

### `Producer`

| Method | Returns | Description |
|---|---|---|
| `publish(key, payload, headers=None, *, delivery_mode=0, seq_num=0, codec=0)` | `int` offset | Publish one message |
| `publish_batch(messages)` | `list[int]` offsets | Publish a list of `Message` objects in one frame |

### `Consumer`

| Method | Returns | Description |
|---|---|---|
| `subscribe()` | `None` | Enter push-delivery mode |
| `messages()` | `Iterator[Message]` | Iterate over push-delivered messages |
| `fetch(partition, offset, max_messages=100, timeout_ms=5000)` | `list[Message]` | Pull fetch |
| `commit(partition, offset)` | `None` | Commit consumer group offset |
| `seek_to_end()` | `dict[int, int]` | Return `{partition: next_offset}` |
| `seek_to_timestamp(timestamp_ns)` | `dict[int, int]` | Seek to first offset ≥ timestamp |
| `reset()` | `None` | Reset all committed offsets to 0 |

### `Message`

| Field | Type | Description |
|---|---|---|
| `id` | `str` | Broker-assigned message UUID |
| `topic` | `str` | Topic name |
| `key` | `str` | Routing / dedup key |
| `payload` | `bytes` | Raw message bytes |
| `headers` | `dict[str, str]` | Metadata headers |
| `offset` | `int` | Logical partition offset |
| `timestamp_ns` | `int` | Unix nanosecond timestamp |
| `partition` | `int` | Partition index |
| `codec` | `int` | Compression codec (0=none, 1=flate, 2=zlib) |

### `BrokerError`

Raised when the broker replies with a `CmdError` frame. Attributes: `code: str`, `message: str`.

## Running Tests

```bash
# Start the broker, then:
python -m pytest python-client/tests/ -v
# Tests auto-skip if the broker is not reachable on 127.0.0.1:9000
```
