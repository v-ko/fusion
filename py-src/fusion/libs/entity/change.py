from __future__ import annotations

from enum import Enum
from typing import Any

from fusion import get_logger
from fusion.libs.entity import Entity, dump_to_dict

log = get_logger(__name__)


class ChangeTypes(Enum):
    EMPTY = 0
    CREATE = 1
    UPDATE = 2
    DELETE = 3


# ChangeData is a tuple of (entity_id, reverse_component, forward_component)
ChangeData = tuple[str, dict[str, Any], dict[str, Any]]


class Change:
    """Represents a single entity-level change using forward/reverse deltas.

    - CREATE: reverse_component is empty, forward_component has full state
    - DELETE: reverse_component has full state, forward_component is empty
    - UPDATE: both have partial diffs (only changed fields)
    """

    def __init__(self, data: ChangeData):
        self._data = data

    @property
    def data(self) -> ChangeData:
        return self._data

    @property
    def entity_id(self) -> str:
        return self._data[0]

    @property
    def reverse_component(self) -> dict[str, Any]:
        return self._data[1]

    @reverse_component.setter
    def reverse_component(self, value: dict[str, Any]):
        self._data = (self._data[0], value, self._data[2])

    @property
    def forward_component(self) -> dict[str, Any]:
        return self._data[2]

    @forward_component.setter
    def forward_component(self, value: dict[str, Any]):
        self._data = (self._data[0], self._data[1], value)

    def type(self) -> ChangeTypes:
        has_reverse = len(self.reverse_component) > 0
        has_forward = len(self.forward_component) > 0
        if has_reverse and has_forward:
            return ChangeTypes.UPDATE
        elif has_reverse:
            return ChangeTypes.DELETE
        elif has_forward:
            return ChangeTypes.CREATE
        else:
            return ChangeTypes.EMPTY

    @property
    def change_type(self) -> ChangeTypes:
        return self.type()

    @staticmethod
    def create(entity: Entity) -> Change:
        return Change((entity.id, {}, dump_to_dict(entity)))

    @staticmethod
    def delete(entity: Entity) -> Change:
        return Change((entity.id, dump_to_dict(entity), {}))

    @staticmethod
    def update(old_entity: Entity, new_entity: Entity) -> Change:
        return old_entity.change_from(new_entity)

    def reversed(self) -> Change:
        return Change((self.entity_id, self.forward_component, self.reverse_component))

    def is_create(self) -> bool:
        return self.type() == ChangeTypes.CREATE

    def is_update(self) -> bool:
        return self.type() == ChangeTypes.UPDATE

    def is_delete(self) -> bool:
        return self.type() == ChangeTypes.DELETE

    def is_empty(self) -> bool:
        return self.type() == ChangeTypes.EMPTY

    def __repr__(self) -> str:
        return (
            f"<Change entity_id={self.entity_id} "
            f"type={self.type()} "
            f"reverse_keys={list(self.reverse_component.keys())} "
            f"forward_keys={list(self.forward_component.keys())}>"
        )
