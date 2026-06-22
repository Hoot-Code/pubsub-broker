"""High-level Producer for publishing messages to the broker."""
from __future__ import annotations

import base64  # PY-14: hoisted to module level — per-call import hit sys.modules
               # on every publish/publish_batch, a needless hot-path overhead.
from typing import Any, Optional

from .client import Client
from .protocol import CMD_BATCH_PUBLISH, CMD_PUBLISH


class ProduceResult:
    """Result of a single publish operation.

    Attributes:
        message_id: Broker-assigned message identifier.
        partition:  Partition the message was written to.
        offset:     Log offset within the partition.
    """

    __slots__ = ("message_id", "partition", "offset")

    def __init__(self, message_id: str, partition: int, offset: int) -> None:
        self.message_id = message_id
        self.partition = partition
        self.offset = offset

    def __repr__(self) -> str:
        return (
            f"ProduceResult(message_id={self.message_id!r}, "
            f"partition={self.partition}, offset={self.offset})"
        )


class Producer(Client):
    """Broker producer client.

    Inherits connection and authentication from :class:`~pubsub.client.Client`.

    Args:
        host:       Broker host.
        port:       Broker TCP port.
        api_key:    API key for authentication.
        client_id:  Logical identifier (default: ``"py-producer"``).
        timeout:    Socket timeout in seconds.
        ping_interval: Keep-alive ping interval in seconds.

    Example::

        with Producer("localhost", 9000, api_key="key") as p:
            result = p.publish("events", b"hello world")
    """

    def __init__(
        self,
        host: str,
        port: int,
        api_key: str = "",
        client_id: str = "py-producer",
        timeout: float = 30.0,
        ping_interval: float = 30.0,
    ) -> None:
        super().__init__(
            host, port, api_key, client_id, timeout, ping_interval
        )

    def publish(
        self,
        topic: str,
        payload: bytes,
        key: str = "",
        headers: Optional[dict] = None,
        delivery_mode: int = 0,
    ) -> ProduceResult:
        """Publish a single message and return the assigned offset.

        Args:
            topic:         Target topic name.
            payload:       Raw message bytes.
            key:           Optional routing key.
            headers:       Optional string key/value metadata.
            delivery_mode: 0=at-most-once, 1=at-least-once, 2=exactly-once.

        Returns:
            :class:`ProduceResult` with message_id, partition, and offset.

        Raises:
            :class:`~pubsub.types.BrokerError`: On broker-side errors.
        """
        body: dict[str, Any] = {
            "topic": topic,
            "payload": base64.b64encode(payload).decode("ascii"),
            "delivery_mode": delivery_mode,
        }
        if key:
            body["key"] = key
        if headers:
            body["headers"] = headers

        resp = self._send_sync(CMD_PUBLISH, body)
        return ProduceResult(
            message_id=resp.get("message_id", ""),
            partition=resp.get("partition", 0),
            offset=resp.get("offset", 0),
        )

    def publish_batch(
        self,
        topic: str,
        messages: list[dict],
    ) -> list[ProduceResult]:
        """Publish multiple messages in a single round-trip.

        Args:
            topic:    Target topic name (all messages must share the same topic).
            messages: List of dicts with keys ``payload`` (bytes), ``key`` (str,
                      optional), ``headers`` (dict, optional), and
                      ``delivery_mode`` (int, optional).

        Returns:
            List of :class:`ProduceResult`, one per input message.

        Raises:
            :class:`~pubsub.types.BrokerError`: On broker-side errors.
        """
        encoded: list[dict] = []
        for m in messages:
            entry: dict[str, Any] = {
                "topic": topic,
                "payload": base64.b64encode(m["payload"]).decode("ascii"),
                "delivery_mode": m.get("delivery_mode", 0),
            }
            if m.get("key"):
                entry["key"] = m["key"]
            if m.get("headers"):
                entry["headers"] = m["headers"]
            encoded.append(entry)

        resp = self._send_sync(CMD_BATCH_PUBLISH, {"messages": encoded})
        results = []
        for r in resp.get("results", []):
            results.append(
                ProduceResult(
                    message_id=r.get("message_id", ""),
                    partition=r.get("partition", 0),
                    offset=r.get("offset", 0),
                )
            )
        return results
