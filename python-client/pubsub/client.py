"""Base TCP client with authentication, ping/pong, and request dispatch.

Architecture (post PY-2 / PY-13):

A single background *reader thread* owns the socket recv path. It is the SOLE
reader of the socket for the lifetime of the connection. Every inbound frame is
classified by the reader thread:

  * CMD_RESPONSE / CMD_ERROR → routed to the pending-response registry entry
    whose request_id matches, resolving the Event the issuing caller is
    waiting on.
  * CMD_PUSH → forwarded to ``_handle_push`` (overridden by Consumer).
  * CMD_PONG → silently discarded.

Because only the reader thread ever calls ``recv``, concurrent request-response
operations (Publish, Commit, …) cannot corrupt the framing. The write lock
(``_send_lock``) protects only the socket *send*; the receive side is handled
by the reader thread plus the registry, so a slow read never blocks an
unrelated writer (PY-13).
"""
from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any, Optional

from .protocol import (
    CMD_AUTH, CMD_ERROR, CMD_PING, CMD_PONG, CMD_PUSH, CMD_RESPONSE,
    encode_frame, parse_body, read_frame,
)
from .types import BrokerError

_logger = logging.getLogger("pubsub.client")


class _PendingResponse:
    """A slot in the pending-response registry (PY-2).

    The caller of ``_send_sync`` creates one of these, registers it under the
    request_id, sends the frame, then waits on ``event``. The reader thread
    fills in ``cmd``/``body`` and sets ``event`` when the matching response
    arrives (or leaves the slot untouched on timeout/mismatch).
    """

    __slots__ = ("event", "cmd", "body", "request_id")

    def __init__(self, request_id: int) -> None:
        self.event = threading.Event()
        self.cmd: Optional[int] = None
        self.body: Optional[bytes] = None
        self.request_id = request_id


class Client:
    """Low-level TCP client for the pubsub broker.

    Handles connection, authentication, ping/pong keep-alive, and
    synchronous request/response dispatch.  Higher-level APIs (Producer,
    Consumer) inherit from this class.

    Args:
        host:       Broker host.
        port:       Broker TCP port.
        api_key:    API key for authentication.
        client_id:  Logical identifier sent with AUTH (default: ``"py-client"``).
        timeout:    Socket timeout in seconds (default: 30).
        ping_interval: Seconds between keep-alive pings (default: 30).
    """

    def __init__(
        self,
        host: str,
        port: int,
        api_key: str = "",
        client_id: str = "py-client",
        timeout: float = 30.0,
        ping_interval: float = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.api_key = api_key
        self.client_id = client_id
        self.timeout = timeout
        self.ping_interval = ping_interval

        self._sock: Optional[socket.socket] = None
        # _send_lock protects ONLY the socket send path (PY-13). It is never
        # held during a recv — the reader thread is the sole reader.
        self._send_lock = threading.Lock()
        # _id_lock serialises the request-id counter (PY-4). A dedicated lock
        # keeps id allocation independent of the send lock so a blocked send
        # cannot stall id assignment for another caller.
        self._id_lock = threading.Lock()
        self._req_id: int = 0

        self._ping_thread: Optional[threading.Thread] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        # _dead is set by the reader thread when it exits due to an error
        # (PY-7), so callers can detect a silently-dead connection.
        self._dead = threading.Event()

        # Pending-response registry (PY-2): request_id → _PendingResponse.
        self._pending: dict[int, _PendingResponse] = {}
        self._pending_lock = threading.Lock()

    # ─── Lifecycle ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Connect to the broker and authenticate.

        Wraps the post-connect setup in try/except so that if authentication
        (or thread startup) fails, the already-opened socket is closed before
        re-raising (PY-3).
        """
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = sock
        self._stop_event.clear()
        self._dead.clear()
        try:
            # Start the reader thread BEFORE auth: it is the sole socket reader
            # and routes the AUTH response back to _send_sync via the registry.
            self._start_reader()
            self._authenticate()
            self._start_ping()
        except BaseException:
            # PY-3: ensure the socket and background threads do not leak if
            # any part of the post-connect setup raises.
            self._stop_event.set()
            try:
                sock.close()
            except OSError:
                pass
            self._sock = None
            self._join_background_threads()
            raise

    def close(self) -> None:
        """Close the connection and stop background threads (PY-5, PY-10)."""
        self._stop_event.set()
        # Closing the socket unblocks the reader thread's recv.
        if self._sock is not None:  # PY-10: guard against None socket.
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        self._join_background_threads()

    def _join_background_threads(self) -> None:
        """Join the reader and ping threads with a bounded timeout (PY-5)."""
        for thr in (self._reader_thread, self._ping_thread):
            if thr is not None and thr.is_alive():
                thr.join(timeout=2.0)
        self._reader_thread = None
        self._ping_thread = None

    def __enter__(self) -> "Client":
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ─── Auth ─────────────────────────────────────────────────────────────────

    def _authenticate(self) -> None:
        """Authenticate via CMD_AUTH.

        Uses ``_send_sync`` so the response's request_id is validated against
        the auth request's id via the registry (PY-24) — a stray frame can no
        longer be misinterpreted as a successful auth.
        """
        body = {"api_key": self.api_key, "client_id": self.client_id}
        # _send_sync raises BrokerError on CMD_ERROR and returns the parsed
        # body on CMD_RESPONSE. We don't need the body for a successful auth.
        self._send_sync(CMD_AUTH, body)

    # ─── Request dispatch ─────────────────────────────────────────────────────

    def _send_sync(self, cmd: int, body: Any = None, timeout: Optional[float] = None) -> Any:
        """Send *cmd* with *body* and return the parsed JSON response body.

        The write lock protects only the send (PY-13). The response is
        delivered by the reader thread through the pending-response registry,
        so this method never holds the lock during a recv.

        Raises:
            BrokerError:       On CMD_ERROR responses.
            ConnectionError:   If the connection is dead or closed.
            TimeoutError:      If no matching response arrives in time.
        """
        if self._sock is None:
            raise ConnectionError("client is not connected")
        if self._dead.is_set():
            raise ConnectionError("client connection is dead")

        req_id = self._next_id()
        frame = encode_frame(cmd, req_id, body)

        pending = _PendingResponse(req_id)
        with self._pending_lock:
            self._pending[req_id] = pending

        try:
            with self._send_lock:
                self._send_raw(frame)
        except Exception:
            # Send failed: drop the pending slot so the reader doesn't resolve
            # a ghost entry.
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise

        # Wait for the reader thread to resolve the matching response (PY-13:
        # this wait happens WITHOUT holding the send lock).
        wait_timeout = timeout if timeout is not None else self.timeout
        got = pending.event.wait(timeout=wait_timeout)
        if not got:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise TimeoutError(
                f"no response for request_id={req_id} within {wait_timeout}s"
            )
        if pending.cmd == CMD_ERROR:
            parsed = parse_body(pending.body) or {}
            raise BrokerError(
                parsed.get("code", "BROKER_ERROR"),
                parsed.get("message", "broker error"),
            )
        return parse_body(pending.body)

    # ─── Reader thread (sole socket reader) ───────────────────────────────────

    def _start_reader(self) -> None:
        self._reader_thread = threading.Thread(
            target=self._reader_loop, daemon=True, name="pubsub-reader"
        )
        self._reader_thread.start()

    def _reader_loop(self) -> None:
        """The sole socket reader for the lifetime of the connection.

        Classifies every inbound frame and routes it:
          * CMD_RESPONSE / CMD_ERROR → resolve the matching pending slot (PY-2).
          * CMD_PUSH                 → ``_handle_push`` (Consumer override).
          * CMD_PONG                 → discard (PY-12).
        The loop sets a short socket timeout so it can periodically check
        ``_stop_event`` (PY-6) and catches specific exceptions (PY-7).
        """
        sock = self._sock
        if sock is None:
            return
        # PY-6: a bounded read timeout lets the loop wake up and check
        # _stop_event instead of blocking forever in recv.
        try:
            sock.settimeout(1.0)
        except OSError:
            pass

        while not self._stop_event.is_set():
            try:
                _version, cmd, req_id, body = read_frame(sock)
            except socket.timeout:
                continue  # PY-6: loop back and re-check _stop_event.
            except BrokerError:
                # Should not happen from read_frame, but be defensive.
                _logger.warning("reader: BrokerError while reading frame", exc_info=True)
                continue
            except (ConnectionError, OSError) as exc:
                # Socket is dead (PY-7): log and exit so callers can detect it.
                if not self._stop_event.is_set():
                    _logger.error("reader: connection lost: %s", exc)
                    self._dead.set()
                break
            except Exception:
                # Unexpected error (decode failure, etc.) — log full traceback
                # and mark the connection dead rather than silently swallowing
                # it (PY-7).
                _logger.exception("reader: unrecoverable error in push/read loop")
                self._dead.set()
                break

            if cmd in (CMD_RESPONSE, CMD_ERROR):
                # PY-2 / PY-11: resolve the pending slot whose request_id
                # matches. A mismatched response (e.g. for an already-timed-out
                # request) is logged and dropped.
                with self._pending_lock:
                    pending = self._pending.pop(req_id, None)
                if pending is not None:
                    pending.cmd = cmd
                    pending.body = body
                    pending.event.set()
                else:
                    _logger.debug(
                        "reader: orphan response for request_id=%s (no waiter)",
                        req_id,
                    )
            elif cmd == CMD_PUSH:
                # Deliver to the push handler (no-op in the base Client;
                # Consumer overrides to decode + invoke the callback).
                try:
                    self._handle_push(body)
                except Exception:
                    _logger.exception("reader: push handler raised")
            elif cmd == CMD_PONG:
                continue  # PY-12: discard keep-alive pong.
            else:
                _logger.debug("reader: discarding unexpected frame cmd=0x%02X", cmd)

    def _handle_push(self, body: bytes) -> None:
        """Handle an inbound CMD_PUSH frame.

        The base Client has no push consumer, so the frame is dropped.
        Consumer overrides this to decode the messages and invoke the
        registered callback.
        """

    # ─── Ping / pong ──────────────────────────────────────────────────────────

    def _start_ping(self) -> None:
        self._ping_thread = threading.Thread(
            target=self._ping_loop, daemon=True, name="pubsub-ping"
        )
        self._ping_thread.start()

    def _ping_loop(self) -> None:
        """Send keep-alive PINGs.

        The ping thread NEVER reads from the socket (PY-2/PY-12): the reader
        thread owns recv and silently discards the CMD_PONG replies. If the
        send fails or the connection is marked dead, the loop exits.
        """
        while not self._stop_event.wait(timeout=self.ping_interval):
            if self._dead.is_set() or self._sock is None:
                break
            try:
                req_id = self._next_id()
                frame = encode_frame(CMD_PING, req_id)
                with self._send_lock:
                    self._send_raw(frame)
            except Exception:
                # Send failed — connection is dying; let the reader thread
                # set _dead and exit.
                break

    # ─── Internals ────────────────────────────────────────────────────────────

    def _next_id(self) -> int:
        """Return the next request id, serialised by a dedicated lock (PY-4)."""
        with self._id_lock:
            self._req_id += 1
            return self._req_id

    def _send_raw(self, data: bytes) -> None:
        sock = self._sock
        if sock is None:
            raise ConnectionError("socket is closed")
        total = 0
        while total < len(data):
            n = sock.send(data[total:])
            if n == 0:
                raise ConnectionError("socket closed during send")
            total += n
