"""High-level Consumer for subscribing to and fetching messages from the broker."""
from __future__ import annotations

import base64
import logging
import threading
from typing import Any, Callable, Optional

from .client import Client
from .protocol import (
    CMD_ACK, CMD_COMMIT_OFFSET, CMD_FETCH, CMD_LIST_TOPICS,
    CMD_NACK, CMD_PUSH, CMD_RESET_GROUP, CMD_RESPONSE, CMD_SEEK,
    CMD_SUBSCRIBE, CMD_UNSUBSCRIBE,
    parse_body,
)

_logger = logging.getLogger("pubsub.consumer")


class Message:
    """A message received from the broker.

    Attributes:
        id:        Broker-assigned UUID for the message (PY-8).
        topic:     Source topic.
        partition: Source partition.
        offset:    Log offset.
        key:       Routing key (may be empty).
        payload:   Raw message bytes (never None — PY-26 normalises to b"").
        headers:   Optional string metadata.
        timestamp: Unix nanoseconds when the message was stored.
    """

    __slots__ = ("id", "topic", "partition", "offset", "key", "payload", "headers", "timestamp")

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: str,
        payload: bytes,
        headers: Optional[dict] = None,
        timestamp: int = 0,
        id: str = "",
    ) -> None:
        self.id = id  # PY-8: broker always sets Message.id.
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        # PY-26: a null payload from JSON would be None; normalise to b"" so
        # callers never hit AttributeError on bytes operations.
        if payload is None:
            payload = b""
        self.payload = payload
        self.headers = headers or {}
        self.timestamp = timestamp

    def __repr__(self) -> str:
        return (
            f"Message(id={self.id!r}, topic={self.topic!r}, partition={self.partition}, "
            f"offset={self.offset}, key={self.key!r})"
        )


def _decode_messages(raw_list: list) -> list[Message]:
    """Convert raw JSON message dicts to :class:`Message` objects.

    PY-26: a null payload is normalised to ``b""`` rather than ``None``.
    """
    out: list[Message] = []
    for m in raw_list or []:
        payload_raw = m.get("payload", b"")
        if payload_raw is None:  # PY-26: null payload → b"".
            payload_raw = b""
        elif isinstance(payload_raw, str):
            payload_raw = base64.b64decode(payload_raw)
        out.append(
            Message(
                topic=m.get("topic", ""),
                partition=m.get("partition", 0),
                offset=m.get("offset", 0),
                key=m.get("key", ""),
                payload=payload_raw,
                headers=m.get("headers"),
                timestamp=m.get("timestamp_ns") or m.get("timestamp", 0),
                id=m.get("id", ""),  # PY-8: populate id from the JSON body.
            )
        )
    return out


class Consumer(Client):
    """Broker consumer client.

    Supports both pull (fetch) and push (server-initiated) delivery.

    Args:
        host:       Broker host.
        port:       Broker TCP port.
        group:      Consumer group name.
        consumer_id: Unique identifier within the group.
        api_key:    API key for authentication.
        client_id:  Logical identifier (default: ``"py-consumer"``).
        timeout:    Socket timeout in seconds.
        ping_interval: Keep-alive ping interval in seconds.

    Example::

        with Consumer("localhost", 9000, group="grp", consumer_id="c1") as c:
            c.subscribe("events")
            for msg in c.fetch("events", partition=0, max_count=10):
                print(msg.offset, msg.payload)
                c.commit("events", partition=0, offset=msg.offset)
    """

    def __init__(
        self,
        host: str,
        port: int,
        group: str = "default",
        consumer_id: str = "consumer-1",
        api_key: str = "",
        client_id: str = "py-consumer",
        timeout: float = 30.0,
        ping_interval: float = 30.0,
    ) -> None:
        super().__init__(
            host, port, api_key, client_id, timeout, ping_interval
        )
        self.group = group
        self.consumer_id = consumer_id
        # PY-9: _push_callback is read by the reader thread and written by
        # start_push/stop_push; protect it with a lock rather than relying on
        # the GIL's attribute-assignment atomicity (an undocumented detail).
        self._push_callback_lock = threading.Lock()
        self._push_callback: Optional[Callable[[list[Message]], None]] = None

    # ─── Subscribe / unsubscribe ──────────────────────────────────────────────

    def subscribe(self, topic: str, push: bool = False) -> None:
        """Subscribe this consumer to *topic*.

        Args:
            topic: Topic name.
            push:  If ``True`` the broker will push messages without waiting
                   for :meth:`fetch` calls.
        """
        body = {
            "topic": topic,
            "group": self.group,
            "consumer_id": self.consumer_id,
            "push": push,
        }
        self._send_sync(CMD_SUBSCRIBE, body)

    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from *topic*."""
        body = {
            "topic": topic,
            "group": self.group,
            "consumer_id": self.consumer_id,
        }
        self._send_sync(CMD_UNSUBSCRIBE, body)

    # ─── Pull fetch ───────────────────────────────────────────────────────────

    def fetch(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        max_count: int = 10,
    ) -> list[Message]:
        """Fetch up to *max_count* messages starting at *offset*.

        Args:
            topic:     Topic to read from.
            partition: Partition index.
            offset:    Log offset to start from.
            max_count: Maximum number of messages to return.

        Returns:
            List of :class:`Message`. Empty if the broker transferred the
            payload out-of-band via RawTransfer (``raw_bytes=true``), since the
            raw bytes are not deserialised as messages (BUG 4 part d).
        """
        body = {
            "topic": topic,
            "group": self.group,
            "partition": partition,
            "offset": offset,
            "max_count": max_count,
        }
        resp = self._send_sync(CMD_FETCH, body)
        if not resp:
            return []
        # BUG 4 (part d): if the broker used the raw zero-copy transfer path,
        # the payload was streamed out-of-band and `messages` is empty/nil —
        # do not attempt to unmarshal it.
        if resp.get("raw_bytes"):
            return []
        return _decode_messages(resp.get("messages") or [])

    # ─── Commit / ack / nack ──────────────────────────────────────────────────

    def commit(self, topic: str, partition: int, offset: int) -> None:
        """Commit *offset* for this consumer group and partition.

        Args:
            topic:     Topic name.
            partition: Partition index.
            offset:    Offset to commit.
        """
        body = {
            "group": self.group,
            "consumer_id": self.consumer_id,
            "topic": topic,
            "partition": partition,
            "offset": offset,
        }
        self._send_sync(CMD_COMMIT_OFFSET, body)

    def ack(self, topic: str, partition: int, offset: int) -> None:
        """Acknowledge successful processing of a message.

        Args:
            topic:     Topic name.
            partition: Partition index.
            offset:    Message offset.
        """
        body = {
            "consumer_id": self.consumer_id,
            "topic": topic,
            "partition": partition,
            "offset": offset,
        }
        self._send_sync(CMD_ACK, body)

    def nack(self, topic: str, partition: int, offset: int, requeue: bool = True) -> None:
        """Negatively-acknowledge a message.

        Args:
            topic:     Topic name.
            partition: Partition index.
            offset:    Message offset.
            requeue:   If ``True`` the broker re-delivers (only to this
                       consumer's group — BUG 8); otherwise DLQ.
        """
        body = {
            "consumer_id": self.consumer_id,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            # BUG 8: include the originating group so the broker's nack-requeue
            # path delivers only to this group, not every subscribed group.
            "group": self.group,
            "requeue": requeue,
        }
        self._send_sync(CMD_NACK, body)

    # ─── Seek ─────────────────────────────────────────────────────────────────

    def seek(
        self,
        topic: str,
        timestamp_ns: int = 0,
        to_end: bool = False,
    ) -> dict:
        """Seek this consumer group to a timestamp or endpoint.

        Args:
            topic:        Topic name.
            timestamp_ns: Unix nanoseconds; 0 means beginning.
            to_end:       If ``True``, seek to the latest offset.

        Returns:
            Dict mapping partition index string to new offset.
        """
        body = {
            "topic": topic,
            "group": self.group,
            "timestamp_ns": timestamp_ns,
            "to_end": to_end,
        }
        resp = self._send_sync(CMD_SEEK, body)
        return resp.get("offsets", {}) if resp else {}

    def reset_group(self, topic: str) -> None:
        """Reset all committed offsets for this group and *topic* to 0.

        Args:
            topic: Topic name.
        """
        body = {"group": self.group, "topic": topic}
        self._send_sync(CMD_RESET_GROUP, body)

    # ─── Topic listing ────────────────────────────────────────────────────────

    def list_topics(self) -> list[str]:
        """Return the list of all topic names known to the broker."""
        resp = self._send_sync(CMD_LIST_TOPICS)
        return [t.get("name", "") for t in (resp or {}).get("topics", [])]

    # ─── Server-push delivery ─────────────────────────────────────────────────

    def start_push(self, callback: Callable[[list[Message]], None]) -> None:
        """Start server-push delivery.

        Registers *callback* to be invoked with a list of :class:`Message`
        objects for each CMD_PUSH frame the broker sends. The reader thread
        (started by :meth:`connect`) is the sole socket reader and calls
        :meth:`_handle_push` for every PUSH frame; this method merely installs
        the callback (PY-2/PY-9).

        Args:
            callback: Callable that receives a list of Message objects.
        """
        with self._push_callback_lock:
            self._push_callback = callback

    def stop_push(self) -> None:
        """Stop server-push delivery by clearing the callback (PY-9).

        The reader thread keeps running (it is owned by the Client and drains
        any remaining frames); subsequent CMD_PUSH frames are dropped.
        """
        with self._push_callback_lock:
            self._push_callback = None

    def _handle_push(self, body: bytes) -> None:
        """Decode a CMD_PUSH frame and invoke the registered callback.

        Called by the reader thread (PY-2). The callback is read under
        ``_push_callback_lock`` so a concurrent ``stop_push`` cannot leave the
        reader invoking a half-cleared reference (PY-9).
        """
        parsed = parse_body(body) or {}
        msgs = _decode_messages(parsed.get("messages", []))
        if not msgs:
            return
        with self._push_callback_lock:
            callback = self._push_callback
        if callback is not None:
            try:
                callback(msgs)
            except Exception:
                _logger.exception("push callback raised; continuing")
