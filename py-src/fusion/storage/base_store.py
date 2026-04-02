from collections.abc import Generator
from typing import Any

from fusion import Entity
from fusion.libs.entity.change import Change
from fusion.libs.entity.delta import Delta

IMMUTABILITY_ERROR_MESSAGE = (
    "Cannot alter an object after it has been added to the "
    "repo. Make a copy of it and pass it to the repo instead."
)


class Store:

    def insert_one(self, entity: Entity) -> Change:
        entity.set_immutable(error_message=IMMUTABILITY_ERROR_MESSAGE)
        raise NotImplementedError

    def find(self, **filter: Any) -> Generator[Any, None, None]:
        raise NotImplementedError

    def find_one(self, **filter: Any) -> Any | None:
        found = self.find(**filter)

        for f in found:
            return f

        return None

    def update_one(self, entity: Entity) -> Change:
        entity.set_immutable(error_message=IMMUTABILITY_ERROR_MESSAGE)
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

    def apply_change(self, change: Change) -> None:
        from fusion.libs.entity import load_from_dict, transformed_entity

        if change.is_create():
            entity = load_from_dict(dict(change.forward_component))
            self.insert_one(entity)
        elif change.is_update():
            entity = self.find_one(id=change.entity_id)
            if entity is None:
                raise ValueError(f"Entity {change.entity_id} not found for update")
            entity = transformed_entity(entity, change)
            self.update_one(entity)
        elif change.is_delete():
            entity = self.find_one(id=change.entity_id)
            if entity is None:
                raise ValueError(f"Entity {change.entity_id} not found for delete")
            self.remove_one(entity)

    def apply_delta(self, delta: Delta) -> None:
        for change in delta.changes():
            self.apply_change(change)
