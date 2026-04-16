from collections import defaultdict
from collections.abc import Generator, Iterable
from typing import Any

from fusion.libs.entity import Entity
from fusion.libs.entity.change import Change
from fusion.libs.entity.delta import Delta
from fusion.storage.base_store import Store
from fusion.util import find_many_by_props


class InMemoryStore(Store):

    def __init__(
        self,
        types_for_cached_type_filtering: tuple[type[Entity], ...] | None = None,
    ) -> None:
        super().__init__()
        self.types_for_cached_type_filtering = types_for_cached_type_filtering

        self._entity_cache: dict[str, Entity] = {}
        self._entity_cache_by_parent: defaultdict[str, set[Entity]] = defaultdict(set)
        self._entity_cache_by_type: defaultdict[type[Entity], set[Entity]] = (
            defaultdict(set)
        )

    def clear(self) -> None:
        """Remove all entities from the store."""
        self._entity_cache.clear()
        self._entity_cache_by_parent.clear()
        self._entity_cache_by_type.clear()
        self._loaded = False

    def type_cache_supported_subclass(self, entity: Entity) -> type[Entity] | None:
        if not self.types_for_cached_type_filtering:
            return None

        for supported_class in self.types_for_cached_type_filtering:
            if isinstance(entity, supported_class):
                return supported_class
        return None

    def upsert_to_cache(self, entity: Entity) -> Entity | None:
        """Adds an entity to the cache. Returns the old entity or None"""
        old_entity = self.pop_from_cache(entity.id)

        # Insert it into the indices
        self._entity_cache[entity.id] = entity

        if self.types_for_cached_type_filtering:
            supported_subclass = self.type_cache_supported_subclass(entity)
            if supported_subclass:
                self._entity_cache_by_type[supported_subclass].add(entity)
        if entity.parent_id:
            self._entity_cache_by_parent[entity.parent_id].add(entity)
        return old_entity

    def pop_from_cache(self, entity_id: str) -> Entity | None:
        entity = self._entity_cache.pop(entity_id, None)
        if not entity:
            return None

        if self.types_for_cached_type_filtering:
            supported_subclass = self.type_cache_supported_subclass(entity)
            if supported_subclass:
                self._entity_cache_by_type[supported_subclass].remove(entity)

        if entity.parent_id:
            self._entity_cache_by_parent[entity.parent_id].remove(entity)

        return entity

    def insert_one(self, entity: Entity) -> Change:
        if entity.id in self._entity_cache:
            raise Exception(
                f"Cannot insert entity with id={entity.id!r} ({type(entity).__name__}), "
                f"since it already exists"
            )

        self.upsert_to_cache(entity)
        change = Change.create(entity)
        if self.on_changes and not self._applying_internally:
            self.on_changes(Delta.from_changes([change]), None)
        return change

    def update_one(self, entity: Entity) -> Change:
        old_entity = self.pop_from_cache(entity.id)
        if not old_entity:
            raise Exception(f"Cannot update missing {entity}")

        self.upsert_to_cache(entity)
        change = Change.update(old_entity, entity)
        if self.on_changes and not self._applying_internally and not change.is_empty():
            self.on_changes(Delta.from_changes([change]), None)
        return change

    def remove_one(self, entity: Entity) -> Change:
        old_entity = self.pop_from_cache(entity.id)
        if not old_entity:
            raise Exception(f"Cannot remove missing {entity}")

        change = Change.delete(old_entity)
        if self.on_changes and not self._applying_internally:
            self.on_changes(Delta.from_changes([change]), None)
        return change

    def find(
        self,
        id: str | None = None,
        type: type[Entity] | None = None,
        parent_id: str | None = None,
        **filter: Any,
    ) -> Generator[Entity, None, None]:
        # If searching by id - there will be only one unique result (if any)
        if id is not None:
            try:
                result = self._entity_cache.get(id)
            except TypeError:
                result = None
            if result:
                yield result
            return

        # If searching by parent_id - use the index to do it efficiently
        search_set: Iterable[Entity]
        if parent_id is not None:
            try:
                search_set = self._entity_cache_by_parent.get(parent_id, set())
            except TypeError:
                search_set = ()
        else:
            search_set = set(self._entity_cache.values())

        # Searching by type is a special case
        if type is not None:
            if (
                self.types_for_cached_type_filtering
                and type in self.types_for_cached_type_filtering
            ):
                type_search_set = self._entity_cache_by_type.get(type, set())
                search_set = set(search_set).intersection(type_search_set)
            else:
                search_set = (e for e in search_set if isinstance(e, type))

        # Apply the rest of the filter
        if filter:
            search_set = find_many_by_props(search_set, **filter)
        yield from search_set
