"""Server-side service for the store-sync protocol.

Receives a store reference and tracks changes via a monotonic seq counter
and a delta ring buffer.  Does NOT own the store or set ``on_changes`` —
the consumer wires persistence / side-effects separately.

This is the server-side counterpart to the TypeScript
``RestStoreSyncClient`` in ``fusion/js-src/src/storage/sync/``.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator

from fusion.libs.entity import dump_to_dict
from fusion.libs.entity.change import Change
from fusion.libs.entity.delta import Delta
from fusion.logging import get_logger
from fusion.storage.in_memory_store import InMemoryStore

log = get_logger(__name__)


# Sentinel yielded when the client's ``after`` is too far behind the ring buffer.
STALE = object()


class StoreSyncService:
    """Generic server-side store-sync service.

    Public methods map to REST / SSE endpoints:

    * ``full_state()``       → ``GET  {endpoint}``
    * ``apply_changes()``    → ``POST {endpoint}/changes``
    * ``subscribe(after)``   → async generator of ``(seq, changes_list)`` tuples
    """

    def __init__(self, store: InMemoryStore, buffer_size: int = 200) -> None:
        self.store = store
        self.delta_log: deque[tuple[int, list]] = deque(maxlen=buffer_size)
        self.seq: int = 0  # Sequential number of the last applied delta
        self._lock = asyncio.Lock()
        self._subscribers: set[asyncio.Queue] = set()

    def full_state(self) -> dict:
        entities = [dump_to_dict(e) for e in self.store.find()]
        return {"seq": self.seq, "entities": entities}

    async def apply_changes(self, changes: list) -> dict:
        """Apply a batch of changes from a client.

        *changes* is a list of wire-format change dicts.

        Returns ``{seq}`` with the sequence number assigned to this delta.
        All subscribers are notified via their queues.
        """
        async with self._lock:
            # Convert wire-format changes to a Delta
            change_objects = [Change(c[0], c[1], c[2]) for c in changes]
            delta = Delta.from_changes(change_objects)
            self.store.apply_delta(delta)

            self.seq += 1
            # Store serialized changes in the ring buffer
            serialized = [c.asdict() for c in change_objects]
            self.delta_log.append((self.seq, serialized))

            # Notify subscribers
            for q in self._subscribers:
                q.put_nowait((self.seq, serialized))

            return {"seq": self.seq}

    # ------------------------------------------------------------------
    # subscribe — async generator of (seq, changes) | STALE
    # ------------------------------------------------------------------

    async def subscribe(self, after: int) -> AsyncGenerator[tuple[int, list] | object]:
        """Async generator yielding ``(seq, changes_list)`` tuples.

        Replays missed deltas from the ring buffer, then streams new
        deltas as they arrive.  Yields the ``STALE`` sentinel (once) if
        the client's *after* is too far behind the ring buffer.
        """
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.add(queue)
        try:
            # Check for stale
            if self.delta_log:
                oldest_seq = self.delta_log[0][0]
                if after < oldest_seq - 1:
                    yield STALE
                    return

            # Replay missed deltas from the ring buffer
            last_replayed = after
            for seq, changes in self.delta_log:
                if seq > after:
                    yield (seq, changes)
                    last_replayed = seq

            # Stream new deltas
            while True:
                seq, changes = await queue.get()
                if seq <= last_replayed:
                    continue
                yield (seq, changes)
                last_replayed = seq
        finally:
            self._subscribers.discard(queue)
