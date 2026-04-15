"""Tests for StoreSyncService — the server-side store-sync protocol."""

import asyncio

from fusion import Entity, entity_type
from fusion.libs.entity.change import Change
from fusion.storage.in_memory_store import InMemoryStore
from fusion.storage.store_sync_service import STALE, StoreSyncService


@entity_type
class TestConfigEntity(Entity):
    name: str = ""
    value: str = ""


def test_full_state_empty():
    store = InMemoryStore()
    sss = StoreSyncService(store)
    state = sss.full_state()
    assert state == {"seq": 0, "entities": []}


def test_full_state_with_entities():
    store = InMemoryStore()
    e = TestConfigEntity(id="e1", name="hello", value="world")
    store.insert_one(e)
    sss = StoreSyncService(store)
    state = sss.full_state()
    assert state["seq"] == 0
    assert len(state["entities"]) == 1
    assert state["entities"][0]["id"] == "e1"
    assert state["entities"][0]["type_name"] == "TestConfigEntity"


def test_apply_changes_create():
    async def _test():
        store = InMemoryStore()
        sss = StoreSyncService(store)

        e = TestConfigEntity(id="e1", name="hello", value="world")
        change = Change.create(e)

        result = await sss.apply_changes([change.asdict()])
        assert result["seq"] == 1

        found = store.find_one(id="e1")
        assert found is not None
        assert found.name == "hello"

    asyncio.run(_test())


def test_apply_changes_update():
    async def _test():
        store = InMemoryStore()
        e = TestConfigEntity(id="e1", name="hello", value="world")
        store.insert_one(e)
        sss = StoreSyncService(store)

        e2 = TestConfigEntity(id="e1", name="updated", value="world")
        change = e.change_from(e2)

        result = await sss.apply_changes([change.asdict()])
        assert result["seq"] == 1

        found = store.find_one(id="e1")
        assert found.name == "updated"

    asyncio.run(_test())


def test_subscribe_replays_missed():
    """Subscribe with after=0 after some changes should replay them."""

    async def _test():
        store = InMemoryStore()
        sss = StoreSyncService(store)

        e = TestConfigEntity(id="e1", name="hello", value="world")
        await sss.apply_changes([Change.create(e).asdict()])

        items = []
        async for item in sss.subscribe(after=0):
            items.append(item)
            break

        assert len(items) == 1
        seq, changes = items[0]
        assert seq == 1
        assert isinstance(changes, list)

    asyncio.run(_test())


def test_subscribe_streams_new():
    """Subscribe then apply changes — subscriber should receive them."""

    async def _test():
        store = InMemoryStore()
        sss = StoreSyncService(store)

        received = []

        async def consumer():
            async for item in sss.subscribe(after=0):
                received.append(item)
                if len(received) >= 1:
                    break

        async def producer():
            await asyncio.sleep(0.01)
            e = TestConfigEntity(id="e1", name="hello", value="world")
            await sss.apply_changes([Change.create(e).asdict()])

        await asyncio.gather(consumer(), producer())

        assert len(received) == 1
        seq, changes = received[0]
        assert seq == 1

    asyncio.run(_test())


def test_subscribe_stale():
    """Subscribe with a too-old after value yields the STALE sentinel."""

    async def _test():
        store = InMemoryStore()
        sss = StoreSyncService(store, buffer_size=2)

        for i in range(3):
            e = TestConfigEntity(id=f"e{i}", name=f"name{i}", value=f"val{i}")
            await sss.apply_changes([Change.create(e).asdict()])

        items = []
        async for item in sss.subscribe(after=0):
            items.append(item)
            break

        assert len(items) == 1
        assert items[0] is STALE

    asyncio.run(_test())


def test_subscribers_notified_on_apply():
    """Multiple deltas are streamed to the subscriber in order."""

    async def _test():
        store = InMemoryStore()
        sss = StoreSyncService(store)

        received = []

        async def consumer():
            async for item in sss.subscribe(after=0):
                received.append(item)
                if len(received) >= 2:
                    break

        async def producer():
            await asyncio.sleep(0.01)
            e1 = TestConfigEntity(id="e1", name="from_a", value="v1")
            await sss.apply_changes([Change.create(e1).asdict()])
            e2 = TestConfigEntity(id="e2", name="from_b", value="v2")
            await sss.apply_changes([Change.create(e2).asdict()])

        await asyncio.gather(consumer(), producer())

        assert len(received) == 2
        assert received[0][0] == 1
        assert received[1][0] == 2

    asyncio.run(_test())


def test_on_changes_fires():
    async def _test():
        store = InMemoryStore()
        sss = StoreSyncService(store)
        received = []
        store.on_changes = lambda delta, origin: received.append((delta, origin))

        e = TestConfigEntity(id="e1", name="hello", value="world")
        await sss.apply_changes([Change.create(e).asdict()])

        assert len(received) == 1
        delta, origin = received[0]
        assert not delta.is_empty()
        assert origin is None

    asyncio.run(_test())
