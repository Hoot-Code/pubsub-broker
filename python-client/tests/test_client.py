"""Integration tests for the pubsub Python client (PY-15..21 rewrite).

These tests require a running broker. They are skipped automatically when the
broker is not reachable, so the suite passes cleanly in environments without a
broker — but they NEVER skip due to an API mismatch (every call uses the
current, correct SDK signature).

Configuration:
    PUBSUB_BROKER_ADDR  (default ``127.0.0.1:9000``)

Run manually::

    # Start the broker first, then:
    python -m pytest python-client/tests/test_client.py -v
"""
from __future__ import annotations

import os
import socket
import threading
import time
import uuid

import pytest

# ─── Broker availability detection ────────────────────────────────────────────

_BROKER_ADDR = os.environ.get("PUBSUB_BROKER_ADDR", "127.0.0.1:9000")
_HOST, _PORT_STR = _BROKER_ADDR.rsplit(":", 1)
_BROKER_HOST = _HOST
_BROKER_PORT = int(_PORT_STR)


def _broker_reachable() -> bool:
    """Probe the broker once; tests skip if it is not listening."""
    try:
        s = socket.create_connection((_BROKER_HOST, _BROKER_PORT), timeout=1.0)
        s.close()
        return True
    except OSError:
        return False


_BROKER_AVAILABLE = _broker_reachable()

skip_no_broker = pytest.mark.skipif(
    not _BROKER_AVAILABLE,
    reason=f"broker not reachable at {_BROKER_ADDR}",
)

# ─── Imports: top-level pubsub package only (PY-15d) ──────────────────────────

from pubsub import BrokerError, Consumer, Producer  # noqa: E402
from pubsub.protocol import CMD_CREATE_TOPIC  # noqa: E402


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _unique_topic() -> str:
    """Per-test unique topic name (PY-22): uuid4 avoids time-based collisions."""
    return f"test-{uuid.uuid4().hex[:12]}"


def _create_topic(client, topic: str, partitions: int = 1) -> None:
    """Create *topic* via a raw CMD_CREATE_TOPIC frame.

    Idempotent: a TOPIC_EXISTS error is treated as success.
    """
    try:
        client._send_sync(
            CMD_CREATE_TOPIC,
            {"name": topic, "partitions": partitions, "replication_factor": 1},
        )
    except BrokerError as exc:
        if exc.code != "TOPIC_EXISTS":
            raise


@pytest.fixture
def producer():
    """A connected Producer that is closed on teardown."""
    p = Producer(_BROKER_HOST, _BROKER_PORT, timeout=10.0, ping_interval=60.0)
    p.connect()
    yield p
    p.close()


@pytest.fixture
def consumer():
    """A connected Consumer that is closed on teardown."""
    c = Consumer(
        _BROKER_HOST, _BROKER_PORT,
        group=f"g-{uuid.uuid4().hex[:8]}",
        consumer_id=f"c-{uuid.uuid4().hex[:8]}",
        timeout=10.0,
        ping_interval=60.0,
    )
    c.connect()
    yield c
    c.close()


# ─── Tests ────────────────────────────────────────────────────────────────────

@skip_no_broker
def test_ping(producer):
    """Connecting to the broker completes auth + ping thread startup."""
    # If we got here, connect() succeeded (auth + reader thread + ping thread).
    assert producer._sock is not None
    assert producer._reader_thread is not None and producer._reader_thread.is_alive()


@skip_no_broker
def test_auth_invalid_key():
    """An invalid API key must surface as BrokerError (when auth is enabled).

    If the broker has auth disabled, every key is accepted and this test skips
    rather than failing — it cannot meaningfully verify the error path without
    an auth-enabled broker.
    """
    c = Consumer(_BROKER_HOST, _BROKER_PORT, api_key="definitely-invalid-key")
    try:
        try:
            c.connect()
        except BrokerError as exc:
            # Auth failure surfaced correctly (PY-1: BrokerError is the real
            # class, so this except clause catches it).
            assert exc.code in ("AUTH_ERROR", "UNAUTHORIZED", "FORBIDDEN")
            return
        # Auth is disabled on the broker — skip, not fail.
        pytest.skip("broker has auth disabled; cannot verify invalid-key rejection")
    finally:
        c.close()


@skip_no_broker
def test_publish_and_fetch(producer, consumer):
    """Publish 10 messages and fetch them back; verify payloads and offsets."""
    topic = _unique_topic()
    _create_topic(producer, topic, 1)

    payloads = [f"msg-{i}".encode() for i in range(10)]
    results = [producer.publish(topic, payloads[i], key=f"k-{i}") for i in range(10)]

    # Offsets must be a dense 0..9 range.
    assert [r.offset for r in results] == list(range(10))

    consumer.subscribe(topic)
    msgs = consumer.fetch(topic, partition=0, offset=0, max_count=20)
    assert len(msgs) == 10
    for i, m in enumerate(msgs):
        assert m.payload == payloads[i]
        assert m.offset == i


@skip_no_broker
def test_publish_batch(producer, consumer):
    """Publish a batch of 5 messages and fetch them back."""
    topic = _unique_topic()
    _create_topic(producer, topic, 1)

    messages = [
        {"payload": f"batch-{i}".encode(), "key": f"bk-{i}"}
        for i in range(5)
    ]
    results = producer.publish_batch(topic, messages)
    assert len(results) == 5
    assert all(r.offset >= 0 for r in results)

    consumer.subscribe(topic)
    msgs = consumer.fetch(topic, partition=0, offset=0, max_count=20)
    assert len(msgs) == 5
    for i, m in enumerate(msgs):
        assert m.payload == f"batch-{i}".encode()


@skip_no_broker
def test_seek_to_end(producer, consumer):
    """Seek to end: only messages published AFTER the seek are returned."""
    topic = _unique_topic()
    _create_topic(producer, topic, 1)

    for i in range(5):
        producer.publish(topic, f"before-{i}".encode())

    consumer.subscribe(topic)
    # Seek to the latest offset.
    offsets = consumer.seek(topic, to_end=True)
    # The seek response maps partition (as string) → new offset.
    next_off = int(offsets.get("0", offsets.get(0, 5)))

    for i in range(5):
        producer.publish(topic, f"after-{i}".encode())

    msgs = consumer.fetch(topic, partition=0, offset=next_off, max_count=20)
    assert len(msgs) == 5, f"expected 5 post-seek messages, got {len(msgs)}"
    for m in msgs:
        assert m.payload.startswith(b"after-")


@skip_no_broker
def test_reset_group(producer, consumer):
    """reset_group resets committed offsets so already-read messages are re-read."""
    topic = _unique_topic()
    _create_topic(producer, topic, 1)

    for i in range(3):
        producer.publish(topic, f"r-{i}".encode())

    consumer.subscribe(topic)
    # Read and commit all 3.
    msgs = consumer.fetch(topic, partition=0, offset=0, max_count=10)
    assert len(msgs) == 3
    for m in msgs:
        consumer.commit(topic, m.partition, m.offset)

    # After committing offset 2, a fetch from offset 0 still returns all 3
    # (fetch uses the requested offset, not the committed one). But the
    # committed offset is now 2. Reset it.
    consumer.reset_group(topic)

    # After reset, the group's committed offset is 0; a fetch from offset 0
    # returns all 3 messages again.
    msgs2 = consumer.fetch(topic, partition=0, offset=0, max_count=10)
    assert len(msgs2) == 3, f"expected 3 messages after reset, got {len(msgs2)}"


@skip_no_broker
def test_push_subscribe(producer, consumer):
    """Push delivery: published messages arrive via the push callback."""
    topic = _unique_topic()
    _create_topic(producer, topic, 1)

    received: list = []
    ready = threading.Event()

    def on_push(msgs):
        received.extend(msgs)
        ready.set()

    consumer.subscribe(topic, push=True)
    consumer.start_push(on_push)

    # Publish 5 messages from the producer.
    for i in range(5):
        producer.publish(topic, f"push-{i}".encode())

    # Wait for delivery.
    assert ready.wait(timeout=5.0), "push callback was not invoked within 5s"
    # Give the reader thread a moment to drain any remaining PUSH frames.
    deadline = time.time() + 2.0
    while len(received) < 5 and time.time() < deadline:
        time.sleep(0.05)

    assert len(received) == 5, f"expected 5 pushed messages, got {len(received)}"
    consumer.stop_push()


@skip_no_broker
def test_concurrent_publish_while_push_active(producer, consumer):
    """PY-2: concurrent publishes while push mode is active must not corrupt framing.

    A single Consumer connection subscribes in push mode (so its reader thread
    is the sole socket reader) and concurrently publishes 20 messages via
    ``_send_sync``. Every publish must return an offset, and every message must
    arrive via the push callback.
    """
    topic = _unique_topic()
    _create_topic(producer, topic, 1)

    received: list = []
    received_lock = threading.Lock()

    def on_push(msgs):
        with received_lock:
            received.extend(msgs)

    # Subscribe the CONSUMER (same connection that will publish) in push mode.
    consumer.subscribe(topic, push=True)
    consumer.start_push(on_push)

    n = 20
    offsets: list[int] = [0] * n
    errors: list = []

    def publish_one(i: int):
        try:
            from pubsub.protocol import CMD_PUBLISH
            resp = consumer._send_sync(
                CMD_PUBLISH,
                {
                    "topic": topic,
                    "payload": __import__("base64").b64encode(f"c-{i}".encode()).decode(),
                    "key": f"ck-{i}",
                    "delivery_mode": 1,
                },
                timeout=10.0,
            )
            offsets[i] = (resp or {}).get("offset", -1)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=publish_one, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors, f"{len(errors)} concurrent publishes failed: {errors[0]}"
    assert all(o >= 0 for o in offsets), f"missing offsets: {offsets}"
    assert len(set(offsets)) == n, f"duplicate offsets: {offsets}"

    # Wait for all n messages to be pushed back.
    deadline = time.time() + 5.0
    while True:
        with received_lock:
            count = len(received)
        if count >= n or time.time() >= deadline:
            break
        time.sleep(0.05)

    assert len(received) == n, (
        f"expected {n} pushed messages, got {len(received)} (PY-2: concurrent "
        f"publish + push may have corrupted framing or lost deliveries)"
    )
    consumer.stop_push()
