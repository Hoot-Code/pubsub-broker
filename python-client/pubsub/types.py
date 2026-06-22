"""types.py — shared data types for the pubsub-broker Python client.

No external dependencies; stdlib only. Supports Python 3.9+.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Message:
    """A single message received from the broker.

    Attributes:
        id:           Broker-assigned UUID for the message.
        topic:        Topic the message belongs to.
        key:          Optional routing / deduplication key.
        payload:      Raw message bytes.
        headers:      Arbitrary string key→value metadata.
        offset:       Logical offset within the partition.
        timestamp_ns: Unix nanosecond timestamp set by the broker.
        partition:    Partition index this message was stored in.
        codec:        Codec identifier (0=none, 1=flate, 2=zlib).
    """

    id: str = ""
    topic: str = ""
    key: str = ""
    payload: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)
    offset: int = 0
    timestamp_ns: int = 0
    partition: int = 0
    codec: int = 0


class BrokerError(Exception):
    """Raised when the broker returns an error response frame.

    Attributes:
        code:    Short error code string (e.g. ``"NOT_FOUND"``).
        message: Human-readable description from the broker.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
