from __future__ import annotations

from copy import deepcopy
from typing import Any, Generator

from fusion import get_logger
from fusion.libs.entity.change import Change, ChangeData, ChangeTypes

log = get_logger(__name__)


# DeltaData is a dict of entity_id -> ChangeData
DeltaData = dict[str, ChangeData]


def _entity_keys_are_equal(value1: Any, value2: Any, depth: int = 1) -> bool:
    """Deep equality check for entity property values, up to 3 levels.

    Max depth matches entity property nesting convention:
    e.g. entity.content.image.height = 3 levels
    (1: property scope, 2: property object, 3: leaf key)
    """
    if depth > 3:
        raise ValueError(
            "Entity key comparison exceeded max depth of 3 — "
            "entity properties should not nest deeper than "
            "property.object.key (e.g. content.image.height)."
        )

    if type(value1) != type(value2):
        return False

    if value1 is None or value2 is None:
        return value1 == value2

    if not isinstance(value1, (dict, list)):
        return value1 == value2

    if isinstance(value1, list):
        if len(value1) != len(value2):
            return False
        return all(
            _entity_keys_are_equal(v1, v2, depth + 1) for v1, v2 in zip(value1, value2)
        )

    # Both are dicts
    if value1.keys() != value2.keys():
        return False
    return all(_entity_keys_are_equal(value1[k], value2[k], depth + 1) for k in value1)


def _change_reduces_to_none(change: Change) -> bool:
    """Check if an update change is a no-op (forward == reverse for all keys)."""
    if not change.is_update():
        return False

    reverse = dict(change.reverse_component)
    forward = dict(change.forward_component)

    for key in list(reverse.keys()):
        if key not in forward:
            continue
        if _entity_keys_are_equal(reverse[key], forward[key]):
            del reverse[key]
            del forward[key]

    change.reverse_component = reverse
    change.forward_component = forward

    return len(reverse) == 0 and len(forward) == 0


class Delta:
    """A collection of Changes grouped by entity ID, with merge logic."""

    def __init__(self, data: DeltaData | None = None):
        self._data: DeltaData = data or {}

    @staticmethod
    def from_changes(changes: list[Change]) -> Delta:
        delta = Delta()
        for change in changes:
            delta.add_change_from_data(change.data)
        return delta

    @property
    def data(self) -> DeltaData:
        return self._data

    def changes(self) -> Generator[Change, None, None]:
        for change_data in self._data.values():
            yield Change(change_data)

    def copy(self) -> Delta:
        return Delta(deepcopy(self._data))

    def reversed(self) -> Delta:
        reversed_data: DeltaData = {}
        for entity_id, (eid, reverse, forward) in self._data.items():
            reversed_data[entity_id] = (eid, forward, reverse)
        return Delta(reversed_data)

    def add_change_from_data(self, change_data: ChangeData):
        entity_id = change_data[0]
        if entity_id in self._data:
            self.merge_change_with_priority(Change(change_data))
        else:
            self._data[entity_id] = change_data

    def remove_change(self, entity_id: str):
        if entity_id not in self._data:
            raise KeyError(f"No change for entity {entity_id}")
        del self._data[entity_id]

    def is_empty(self) -> bool:
        for _, reverse, forward in self._data.values():
            if len(reverse) > 0 or len(forward) > 0:
                return False
        return True

    def entity_ids(self) -> list[str]:
        return list(self._data.keys())

    def change_data(self, entity_id: str) -> ChangeData | None:
        return self._data.get(entity_id)

    def change(self, entity_id: str) -> Change | None:
        cd = self.change_data(entity_id)
        if cd is None:
            return None
        return Change(cd)

    def merge_change_with_priority(self, change: Change):
        """Merge a new change into the existing delta for the same entity.

        Supported merge patterns:
        1) Update > Update: combine forward/reverse fields
        2) Create > Update: extend the create's forward component
        3) Delete > Create: becomes an update (old → new)
        4) Create > Delete: cancels out (removed entirely)
        5) Otherwise: log error
        """
        first_change = self.change(change.entity_id)

        if first_change is None:
            self.add_change_from_data(change.data)
            return

        first_ct = first_change.type()
        next_ct = change.type()

        # 1) Update > Update
        if first_ct == ChangeTypes.UPDATE and next_ct == ChangeTypes.UPDATE:
            merged_forward = {
                **first_change.forward_component,
                **change.forward_component,
            }
            merged_reverse = {
                **change.reverse_component,
                **first_change.reverse_component,
            }
            first_change.forward_component = merged_forward
            first_change.reverse_component = merged_reverse
            if _change_reduces_to_none(first_change):
                self.remove_change(change.entity_id)
            else:
                self._data[change.entity_id] = first_change.data

        # 2) Create > Update
        elif first_ct == ChangeTypes.CREATE and next_ct == ChangeTypes.UPDATE:
            merged_forward = {
                **first_change.forward_component,
                **change.forward_component,
            }
            first_change.forward_component = merged_forward
            self._data[change.entity_id] = first_change.data

        # 3) Delete > Create
        elif first_ct == ChangeTypes.DELETE and next_ct == ChangeTypes.CREATE:
            old_data = first_change.reverse_component
            new_data = change.forward_component
            self._data[change.entity_id] = (change.entity_id, old_data, new_data)
            merged_change = self.change(change.entity_id)
            if _change_reduces_to_none(merged_change):
                self.remove_change(change.entity_id)
            else:
                self._data[change.entity_id] = merged_change.data

        # 4) Create > Delete
        elif first_ct == ChangeTypes.CREATE and next_ct == ChangeTypes.DELETE:
            self.remove_change(change.entity_id)

        # 5) Empty or irrational
        elif next_ct == ChangeTypes.EMPTY:
            pass
        else:
            log.error(
                f"[merge_change_with_priority] Irrational delta sequence. "
                f"Merging {change} INTO {first_change}"
            )

    def merge_with_priority(self, next_delta: Delta):
        """Merge another delta into this one, in place."""
        for change in next_delta.changes():
            self.merge_change_with_priority(change)


def squash_deltas(deltas: list[DeltaData]) -> Delta:
    """Reduce an array of DeltaData dicts into one Delta."""
    squashed = Delta()
    for delta_data in deltas:
        squashed.merge_with_priority(Delta(delta_data))
    return squashed
