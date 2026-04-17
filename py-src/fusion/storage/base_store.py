from collections.abc import Callable, Generator
from typing import Any

from fusion import Entity
from fusion.libs.model import load_from_dict, transformed_entity
from fusion.storage.change import Change
from fusion.storage.delta import Delta


class Store:
    on_changes: Callable[[Delta, str | None], None] | None = None
    _applying_internally: bool = False

    def insert_one(self, entity: Entity) -> Change:
        raise NotImplementedError

    def find(self, **filter: Any) -> Generator[Any, None, None]:
        raise NotImplementedError

    def find_one(self, **filter: Any) -> Any | None:
        found = self.find(**filter)

        for f in found:
            return f

        return None

    def update_one(self, entity: Entity) -> Change:
        raise NotImplementedError

    def remove_one(self, entity: Entity) -> Change:
        raise NotImplementedError

    # Batch operations (inefficient implementations)
    def insert(self, batch: list[Entity]) -> list[Change]:
        return [self.insert_one(entity) for entity in batch]

    def remove(self, batch: list[Entity]) -> list[Change]:
        return [self.remove_one(entity) for entity in batch]

    def update(self, batch: list[Entity]) -> list[Change]:
        return [self.update_one(entity) for entity in batch]

    def apply_change(self, change: Change, origin: str | None = None) -> Change:
        self._applying_internally = True
        try:
            applied = self._apply_change_core(change)
        finally:
            self._applying_internally = False

        if self.on_changes and not applied.is_empty():
            self.on_changes(Delta.from_changes([applied]), origin)
        return applied

    def clear(self) -> None:
        raise NotImplementedError

    _loaded: bool = False

    def load_data(self, entities: list[Entity], origin: str | None = None) -> Delta:
        """Load initial data into an empty (or cleared) store. Can only be called once, or again after clear()."""
        if self._loaded:
            raise RuntimeError(
                "Store already has data loaded. Call clear() before loading again."
            )
        self._applying_internally = True
        changes: list[Change] = []
        try:
            for entity in entities:
                changes.append(self.insert_one(entity))
        finally:
            self._applying_internally = False
            self._loaded = True

        delta = Delta.from_changes(changes)
        if self.on_changes and not delta.is_empty():
            self.on_changes(delta, origin)
        return delta

    def apply_delta(self, delta: Delta, origin: str | None = None) -> Delta:
        """Apply all changes in a Delta to the store. Returns the applied delta."""
        self._applying_internally = True

        applied = Delta()
        try:
            for change in delta.changes():
                applied_change = self._apply_change_core(change)
                applied.add_change(applied_change)
        finally:
            self._applying_internally = False

        if self.on_changes and not applied.is_empty():
            self.on_changes(applied, origin)
        return applied

    def _apply_change_core(self, change: Change) -> Change:
        """Apply a single change without firing on_changes."""
        if change.is_create():
            entity = load_from_dict(dict(change.forward_component))
            return self.insert_one(entity)
        elif change.is_update():
            entity = self.find_one(id=change.entity_id)
            if entity is None:
                raise ValueError(f"Entity {change.entity_id} not found for update")
            entity = transformed_entity(entity, change)
            return self.update_one(entity)
        elif change.is_delete():
            entity = self.find_one(id=change.entity_id)
            if entity is None:
                raise ValueError(f"Entity {change.entity_id} not found for delete")
            return self.remove_one(entity)
        return change
