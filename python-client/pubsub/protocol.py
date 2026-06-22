"""Wire protocol for the pubsub broker.

Frame layout (all little-endian)::

    ┌──────────────┬──────────┬──────────┬──────────────┬────────────┐
    │ Magic   (4B) │ Ver (1B) │ Cmd (1B) │ ReqID   (8B) │ Len   (4B) │
    └──────────────┴──────────┴──────────┴──────────────┴────────────┘
    │ Body (Len bytes, UTF-8 JSON)                                     │
    └──────────────────────────────────────────────────────────────────┘

Header total: 18 bytes.  All integers are little-endian.
"""
from __future__ import annotations

import json
import struct
from typing import Any

# ─── Constants ────────────────────────────────────────────────────────────────

#: 4-byte magic: 0x50534201 little-endian == b'\x01\x42\x53\x50'
MAGIC: int = 0x50534201
VERSION: int = 0x01
HEADER_SIZE: int = 18

# struct format: magic(I=uint32) ver(B=uint8) cmd(B=uint8) reqID(Q=uint64) bodyLen(I=uint32)
# total = 4 + 1 + 1 + 8 + 4 = 18 bytes
_HEADER_FMT: str = "<IBBQI"

# ─── Command codes (match Go internal/protocol/protocol.go) ──────────────────

CMD_PUBLISH: int       = 0x01
CMD_SUBSCRIBE: int     = 0x02
CMD_UNSUBSCRIBE: int   = 0x03
CMD_FETCH: int         = 0x04
CMD_ACK: int           = 0x05
CMD_NACK: int          = 0x06
CMD_COMMIT_OFFSET: int = 0x07
CMD_CREATE_TOPIC: int  = 0x08
CMD_DELETE_TOPIC: int  = 0x09
CMD_LIST_TOPICS: int   = 0x0A
CMD_AUTH: int          = 0x0B
CMD_PING: int          = 0x0C
CMD_PONG: int          = 0x0D
CMD_RESPONSE: int      = 0x0E
CMD_ERROR: int         = 0x0F
CMD_BATCH_PUBLISH: int = 0x10
# CmdPush is 0x20 in the Go source (internal/protocol/protocol.go line: CmdPush Command = 0x20)
CMD_PUSH: int          = 0x20
CMD_SEEK: int          = 0x30
CMD_RESET_GROUP: int   = 0x31

# ─── Frame encode / decode ────────────────────────────────────────────────────


def encode_frame(cmd: int, request_id: int, body: Any = None) -> bytes:
    """Encode *body* as a JSON-framed protocol message.

    Args:
        cmd:        One of the CMD_* constants above.
        request_id: Caller-chosen correlation ID (uint64).
        body:       JSON-serialisable Python object, or *None* for no body.

    Returns:
        The complete frame bytes (header + body).

    Raises:
        ValueError: If *cmd* is not a known CMD_* constant or *request_id*
            is outside the uint64 range (PY-25).
    """
    # PY-25: validate cmd is a known command code and request_id fits in uint64.
    _validate_cmd(cmd)
    if not 0 <= request_id <= 0xFFFFFFFFFFFFFFFF:
        raise ValueError(
            f"request_id out of uint64 range: {request_id}"
        )

    if body is None:
        body_bytes = b""
    else:
        body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")

    header = struct.pack(
        _HEADER_FMT,
        MAGIC,      # I – uint32
        VERSION,    # B – uint8
        cmd,        # B – uint8
        request_id, # Q – uint64
        len(body_bytes),  # I – uint32
    )
    return header + body_bytes


# _KNOWN_CMDS is the set of all CMD_* constants defined in this module. It is
# populated below the constant declarations and used by _validate_cmd (PY-25)
# to reject frames with unknown command bytes.
_KNOWN_CMDS: frozenset[int] = frozenset(
    {
        CMD_PUBLISH, CMD_SUBSCRIBE, CMD_UNSUBSCRIBE, CMD_FETCH, CMD_ACK,
        CMD_NACK, CMD_COMMIT_OFFSET, CMD_CREATE_TOPIC, CMD_DELETE_TOPIC,
        CMD_LIST_TOPICS, CMD_AUTH, CMD_PING, CMD_PONG, CMD_RESPONSE,
        CMD_ERROR, CMD_BATCH_PUBLISH, CMD_PUSH, CMD_SEEK, CMD_RESET_GROUP,
    }
)


def _validate_cmd(cmd: int) -> None:
    """Raise ValueError if *cmd* is not a known command code (PY-25)."""
    if cmd not in _KNOWN_CMDS:
        raise ValueError(f"unknown command code: 0x{cmd:02X}")


def decode_frame(data: bytes) -> tuple[int, int, int, bytes]:
    """Decode a complete frame from *data*.

    Args:
        data: Raw bytes containing at least HEADER_SIZE bytes.

    Returns:
        A 4-tuple ``(version, cmd, request_id, body_bytes)`` after validating
        the magic prefix. (PY-23: the return type is a 4-tuple, not 5 — the
        magic is validated and discarded internally.)

    Raises:
        ValueError: If the magic bytes are wrong or data is too short.
    """
    if len(data) < HEADER_SIZE:
        raise ValueError(
            f"frame too short: expected >= {HEADER_SIZE} bytes, got {len(data)}"
        )
    magic, version, cmd, request_id, body_len = struct.unpack(
        _HEADER_FMT, data[:HEADER_SIZE]
    )
    if magic != MAGIC:
        raise ValueError(
            f"invalid magic: 0x{magic:08X} (expected 0x{MAGIC:08X})"
        )
    body = data[HEADER_SIZE : HEADER_SIZE + body_len]
    return version, cmd, request_id, body


def read_frame(sock) -> tuple[int, int, int, bytes]:
    """Read exactly one frame from *sock* (a socket or file-like object).

    Reads the 18-byte header, then reads exactly *body_len* body bytes.

    Returns:
        A 4-tuple ``(version, cmd, request_id, body_bytes)``. (PY-23: the
        return type is a 4-tuple; the magic is validated and discarded.)

    Raises:
        ConnectionError: If the connection closes unexpectedly.
        ValueError:      If the magic bytes are invalid.
    """
    raw_header = _recv_exact(sock, HEADER_SIZE)
    magic, version, cmd, request_id, body_len = struct.unpack(
        _HEADER_FMT, raw_header
    )
    if magic != MAGIC:
        raise ValueError(
            f"invalid magic: 0x{magic:08X} (expected 0x{MAGIC:08X})"
        )
    body = _recv_exact(sock, body_len) if body_len > 0 else b""
    return version, cmd, request_id, body


def parse_body(body: bytes) -> Any:
    """Deserialise a JSON body, returning *None* for an empty body."""
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


# ─── Internal helpers ─────────────────────────────────────────────────────────


def _recv_exact(sock, n: int) -> bytes:
    """Read exactly *n* bytes from *sock*, raising on premature close."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(
                f"connection closed after {len(buf)} / {n} bytes"
            )
        buf.extend(chunk)
    return bytes(buf)
