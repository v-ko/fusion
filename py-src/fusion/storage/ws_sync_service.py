"""Bidirectional WebSocket store sync.

A single class that operates as either *authority* (owns the canonical
state, sends ``full_state`` on connect) or *receiver* (expects
``full_state`` on connect, applies it).  After the initial handshake
both roles are **symmetric**: local deltas are sent and ACKed, remote
deltas are received, applied, and ACKed.

Transport-agnostic — the caller provides async ``send`` / ``receive``
callables, so the class works with any WebSocket library (FastAPI,
websockets, aiohttp, etc.).

Wire protocol (JSON messages)::

    {"type": "full_state", "seq": <int>, "entities": [...]}
    {"type": "delta",      "seq": <int>, "delta": {DeltaData}}
    {"type": "ack",        "seq": <int>}
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from fusion.libs.entity import dump_to_dict, load_from_dict
from fusion.libs.entity.delta import Delta, DeltaData
from fusion.logging import get_logger
from fusion.storage.in_memory_store import InMemoryStore

log = get_logger(__name__)

# Type aliases for the transport callables.
SendFn = Callable[[dict[str, Any]], Awaitable[None]]
ReceiveFn = Callable[[], Awaitable[dict[str, Any]]]

ACK_TIMEOUT = 5.0  # seconds


class ProtocolError(Exception):
    """Raised on protocol-level errors during the WebSocket sync."""


@dataclass
class _PendingAck:
    """Tracks an outbound delta waiting for acknowledgement."""

    future: asyncio.Future[None]
    event: threading.Event = field(default_factory=threading.Event)


class WebSocketSyncService:
    """Bidirectional store sync over a WebSocket connection.

    Usage::

        sync = WebSocketSyncService(store, role='authority')
        store.on_changes = sync.on_store_changes   # wire the callback
        await sync.run(ws_send, ws_receive)         # blocks until disconnect
    """

    def __init__(
        self,
        store: InMemoryStore,
        role: str,  # 'authority' | 'receiver'
    ) -> None:
        if role not in ("authority", "receiver"):
            raise ValueError(f"role must be 'authority' or 'receiver', got {role!r}")

        self.store = store
        self.role = role

        self._lock = threading.Lock()  # protects _seq
        self._seq: int = 0
        self._outbound: asyncio.Queue[tuple[int, DeltaData, threading.Event]] = (
            asyncio.Queue()
        )
        self._pending_acks: dict[int, _PendingAck] = {}
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Internal: enqueue a delta for sending
    # ------------------------------------------------------------------

    def _enqueue_delta(self, delta_data: DeltaData) -> threading.Event:
        """Create an event, bump seq, enqueue to outbound. Thread-safe."""
        event = threading.Event()
        with self._lock:
            self._seq += 1
            seq = self._seq

        item = (seq, delta_data, event)
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._outbound.put_nowait, item)
        else:
            self._outbound.put_nowait(item)

        return event

    def _resolve_event(self, event: threading.Event) -> None:
        """Signal that one outbound delta has been ACKed."""
        event.set()

    # ------------------------------------------------------------------
    # Store callback (blocking until ACKed)
    # ------------------------------------------------------------------

    def on_store_changes(self, delta: Delta, origin: str | None = None) -> None:
        """Chain into ``store.on_changes``.

        Enqueues outbound deltas and blocks until the remote peer ACKs.
        Skips remote-origin deltas (echo prevention).  Safe to call
        from any thread (the async send loop runs on the event loop).
        """
        if origin == "remote" or not self._running:
            return
        event = self._enqueue_delta(delta.asdict())
        if not event.wait(ACK_TIMEOUT):
            raise TimeoutError("on_store_changes: ACK timeout")

    # ------------------------------------------------------------------
    # Explicit async push
    # ------------------------------------------------------------------

    async def push_delta(self, delta: Delta) -> None:
        """Send a delta and await until ACKed (async).

        Must be called from the same event loop as ``run()``.
        """
        if not self._running:
            raise RuntimeError("WebSocketSyncService is not running")
        event = self._enqueue_delta(delta.asdict())
        # Poll until the event is set (bridging threading.Event → async)
        while not event.is_set():
            await asyncio.sleep(0.01)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self, send: SendFn, receive: ReceiveFn) -> None:
        """Run the sync protocol on an established connection.

        Blocks until the connection is closed or an unrecoverable error
        occurs.

        Args:
            send:    Async callable — send a JSON-serialisable dict.
            receive: Async callable — return the next parsed JSON message.
        """
        self._loop = asyncio.get_running_loop()
        self._running = True

        try:
            # --- Handshake ---
            if self.role == "authority":
                await send(self._full_state_message())
            else:
                msg = await receive()
                if msg.get("type") != "full_state":
                    raise ProtocolError(f"Expected full_state, got {msg.get('type')!r}")
                self._apply_full_state(msg)

            # --- Concurrent send + receive ---
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._send_loop(send))
                tg.create_task(self._receive_loop(send, receive))
        finally:
            self._running = False
            self._loop = None
            # Cancel pending ACKs so flush callers don't hang
            for pa in self._pending_acks.values():
                if not pa.future.done():
                    pa.future.cancel()
                self._resolve_event(pa.event)
            self._pending_acks.clear()

    # ------------------------------------------------------------------
    # Internal loops
    # ------------------------------------------------------------------

    async def _send_loop(self, send: SendFn) -> None:
        while self._running:
            seq, delta_data, event = await self._outbound.get()

            assert self._loop is not None
            future: asyncio.Future[None] = self._loop.create_future()
            self._pending_acks[seq] = _PendingAck(future=future, event=event)

            await send({"type": "delta", "seq": seq, "delta": delta_data})

            try:
                await asyncio.wait_for(future, timeout=ACK_TIMEOUT)
            except asyncio.TimeoutError:
                log.error("ACK timeout for seq %d", seq)
                pa = self._pending_acks.pop(seq, None)
                if pa is not None:
                    self._resolve_event(pa.event)
                raise

    async def _receive_loop(
        self,
        send: SendFn,
        receive: ReceiveFn,
    ) -> None:
        while self._running:
            msg = await receive()
            msg_type = msg.get("type")

            if msg_type == "delta":
                delta = Delta.from_data(msg["delta"])
                log.info(
                    "WS %s received delta (seq=%s): %s",
                    self.role,
                    msg.get("seq"),
                    list(msg["delta"].keys()),
                )
                self.store.apply_delta(delta, origin="remote")
                await send({"type": "ack", "seq": msg["seq"]})
                with self._lock:
                    self._seq = max(self._seq, msg["seq"])

            elif msg_type == "ack":
                pa = self._pending_acks.pop(msg["seq"], None)
                if pa is not None:
                    if not pa.future.done():
                        pa.future.set_result(None)
                    self._resolve_event(pa.event)

            elif msg_type == "full_state":
                self._apply_full_state(msg)

            else:
                log.warning("Unknown WS message type: %s", msg_type)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _full_state_message(self) -> dict[str, Any]:
        entities = [dump_to_dict(e) for e in self.store.find()]
        return {"type": "full_state", "seq": self._seq, "entities": entities}

    def _apply_full_state(self, msg: dict[str, Any]) -> None:
        with self._lock:
            self._seq = msg.get("seq", 0)
        entities = [load_from_dict(d) for d in msg.get("entities", [])]
        self.store.clear()
        self.store.load_data(entities, origin="remote")
