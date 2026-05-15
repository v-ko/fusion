"""Bidirectional WebSocket store synchronization.

Roles
-----
- **authority**: owns the canonical state; sends ``full_state`` on connect.
- **receiver**: expects ``full_state`` on connect; applies it to its store.

After the initial handshake both sides are symmetric — local deltas are
sent and ACKed, remote deltas are received, applied, and ACKed.

Wire protocol (JSON)::

    {"type": "full_state", "seq": <int>, "entities": [...]}
    {"type": "delta",      "seq": <int>, "delta": {DeltaData}}
    {"type": "ack",        "seq": <int>}

Store interaction
-----------------
- Authority builds ``full_state`` via ``store.find()``.
- Receiver hydrates via ``store.clear()`` + ``store.load_data()``.
  ``load_data`` does **not** fire ``on_changes``, so only real
  user-action deltas reach change listeners.
- Both sides apply remote deltas with ``store.apply_delta(delta,
  origin='remote')``.

Transport-agnostic: the caller supplies async ``send``/``receive``
callables, so it works with any WebSocket library.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from fusion.libs.model import dump_to_dict, load_from_dict
from fusion.logging import get_logger
from fusion.storage.base_store import Store
from fusion.storage.delta import Delta, DeltaData

log = get_logger(__name__)

# Type aliases for the transport callables.
SendFn = Callable[[dict[str, Any]], Awaitable[None]]
ReceiveFn = Callable[[], Awaitable[dict[str, Any]]]

ACK_TIMEOUT = 5.0  # seconds


class ProtocolError(Exception):
    """Raised on protocol-level errors during the WebSocket sync."""


class WebSocketSyncService:
    """Bidirectional store sync over a WebSocket connection.

    Usage (authority)::

        sync = WebSocketSyncService(store, role='authority')
        await sync.run(ws_send, ws_receive)

    Usage (receiver)::

        sync = WebSocketSyncService(store, role='receiver')
        store.on_changes = lambda delta, origin: handle(delta)
        await sync.run(ws_send, ws_receive)
    """

    def __init__(
        self,
        store: Store,
        role: str,  # 'authority' | 'receiver'
        on_ready: Callable[[], None] | None = None,
    ) -> None:
        if role not in ("authority", "receiver"):
            raise ValueError(f"role must be 'authority' or 'receiver', got {role!r}")

        self._store = store
        self.role = role
        self._on_ready = on_ready

        self._seq: int = 0
        self._outbound: asyncio.Queue[tuple[int, DeltaData, asyncio.Future[None]]] = (
            asyncio.Queue()
        )
        self._pending_acks: dict[int, asyncio.Future[None]] = {}
        self.running = False
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    def push_delta(self, delta: Delta) -> asyncio.Future[None]:
        """Enqueue a delta for sending. Returns a Future that resolves on ACK.

        Fire-and-forget is fine — ignore the returned future if you don't
        need ACK confirmation. Must be called from the event loop thread.
        """
        self._seq += 1
        seq = self._seq

        future: asyncio.Future[None]
        if self._loop is not None:
            future = self._loop.create_future()
        else:
            future = asyncio.get_event_loop().create_future()

        if not self.running:
            future.cancel()
            return future

        self._outbound.put_nowait((seq, delta.asdict(), future))
        return future

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self, send: SendFn, receive: ReceiveFn) -> None:
        """Run the sync protocol on an established connection.

        Automatically wires the store's on_changes callback to push local
        deltas outbound (skipping remote-origin). The callback is removed
        on exit.

        Blocks until the connection is closed or an unrecoverable error
        occurs.

        Args:
            send:    Async callable — send a JSON-serialisable dict.
            receive: Async callable — return the next parsed JSON message.
        """
        if self.running:
            raise RuntimeError(
                "run() called on an already-running WebSocketSyncService. "
                "Each instance supports only one concurrent connection."
            )

        self._loop = asyncio.get_running_loop()
        self.running = True

        def _outbound(delta: Delta, origin: str | None = None) -> None:
            if origin == "remote" or not self.running:
                return
            self.push_delta(delta)

        self._store.add_on_changes_callback(_outbound)

        try:
            # --- Handshake ---
            if self.role == "authority":
                await send(self._full_state_message())
            else:
                msg = await receive()
                if msg.get("type") != "full_state":
                    raise ProtocolError(f"Expected full_state, got {msg.get('type')!r}")
                self._handle_full_state(msg)

            # Signal that handshake is complete and store is hydrated
            if self._on_ready is not None:
                self._on_ready()

            # --- Concurrent send + receive ---
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._send_loop(send))
                tg.create_task(self._receive_loop(send, receive))
        finally:
            self._store.remove_on_changes_callback(_outbound)
            self.running = False
            self._loop = None
            # Cancel pending ACKs so callers don't hang
            for future in self._pending_acks.values():
                if not future.done():
                    future.cancel()
            self._pending_acks.clear()

    # ------------------------------------------------------------------
    # Internal loops
    # ------------------------------------------------------------------

    async def _send_loop(self, send: SendFn) -> None:
        while self.running:
            seq, delta_data, future = await self._outbound.get()

            self._pending_acks[seq] = future

            await send({"type": "delta", "seq": seq, "delta": delta_data})

            try:
                await asyncio.wait_for(asyncio.shield(future), timeout=ACK_TIMEOUT)
            except asyncio.TimeoutError:
                log.error("ACK timeout for seq %d", seq)
                pa = self._pending_acks.pop(seq, None)
                if pa is not None and not pa.done():
                    pa.cancel()
                raise

    async def _receive_loop(
        self,
        send: SendFn,
        receive: ReceiveFn,
    ) -> None:
        while self.running:
            msg = await receive()
            msg_type = msg.get("type")

            if msg_type == "delta":
                delta = Delta.from_data(msg["delta"])
                self._store.apply_delta(delta, origin="remote")
                await send({"type": "ack", "seq": msg["seq"]})
                self._seq = max(self._seq, msg["seq"])

            elif msg_type == "ack":
                future = self._pending_acks.pop(msg["seq"], None)
                if future is not None and not future.done():
                    future.set_result(None)

            elif msg_type == "full_state":
                self._handle_full_state(msg)

            else:
                log.warning("Unknown WS message type: %s", msg_type)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _full_state_message(self) -> dict[str, Any]:
        entities = [dump_to_dict(e) for e in self._store.find()]
        return {"type": "full_state", "seq": self._seq, "entities": entities}

    def _handle_full_state(self, msg: dict[str, Any]) -> None:
        self._seq = msg.get("seq", 0)
        entities = [load_from_dict(d) for d in msg.get("entities", [])]
        self._store.clear()
        self._store.load_data(entities, origin="remote")
