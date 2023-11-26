from typing import List, Union
from fusion import Entity
from fusion.libs.entity.change import Change

IMMUTABILITY_ERROR_MESSAGE = (
    'Cannot alter an object after it has been added to the '
    'repo. Make a copy of it and pass it to the repo instead.')


class Repository:

    def insert_one(self, entity: Entity) -> Change:
        entity.set_immutable(error_message=IMMUTABILITY_ERROR_MESSAGE)
        raise NotImplementedError

    def find(self, **filter) -> Entity:
        raise NotImplementedError

    def find_one(self, **filter) -> List[Entity]:
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
    def insert(self, batch: List[Entity]) -> List[Change]:
        return [self.insert_one(entity) for entity in batch]

    def remove(self, batch: List[Entity]) -> List[Change]:
        return [self.remove_one(entity) for entity in batch]

    def update(self, batch: List[Entity]) -> List[Change]:
        return [self.update_one(entity) for entity in batch]
