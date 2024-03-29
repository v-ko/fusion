from __future__ import annotations

from copy import copy
from dataclasses import fields
from datetime import datetime
from enum import Enum
from typing import Any, Generator, Iterable, Optional

from pydantic import BaseModel

from fusion import get_logger
from fusion.libs.entity import dump_to_dict, load_from_dict, Entity
from fusion.util import current_time, get_new_id, timestamp as fusion_timestamp

log = get_logger(__name__)


class DiffTypes(Enum):
    ADDED = 1
    REMOVED = 2


class Diff:

    def __init__(self, change: Change, type: DiffTypes):
        self.change = change
        self.type = type

    def return_added(self, old_val, new_val):
        if not isinstance(old_val, Iterable) and isinstance(new_val, Iterable):
            raise Exception('Attribute type is not Iterable')

        for item in new_val:
            if item not in old_val:
                yield item

    def return_removed(self, old_val, new_val):
        if not isinstance(old_val, Iterable) and isinstance(new_val, Iterable):
            raise Exception('Attribute type is not Iterable')

        for item in old_val:
            if item not in new_val:
                yield item

    def __getattr__(self, key) -> Generator[Any, None, None]:
        if not hasattr(self.change.new_state, key):
            raise AttributeError

        if self.change.is_create():
            pass
        elif self.change.is_delete():
            # Warning and empty
            log.error('Trying to get list/set diff for a deleted state.')
            # yield from []
        else:  # change.is_update()
            # if not hasattr(self.change.old_state, key):
            #     raise AttributeError
            pass

        old_val = getattr(self.change.old_state, key, [])
        new_val = getattr(self.change.new_state, key, [])

        if self.type == DiffTypes.ADDED:
            yield from self.return_added(old_val, new_val)

        else:  # self.type == DiffTypes.REMOVED:
            yield from self.return_removed(old_val, new_val)


class Updated:

    def __init__(self, change: Change):
        self.change = change

    def __getattr__(self, key):
        if self.change.is_create():
            if not hasattr(self.change.new_state, key):
                raise AttributeError
            else:
                return True

        elif self.change.is_delete():
            # Warning and empty
            # log.error(f'Trying to infer if an attribute is updated for a'
            #           f' deleted state. Change: {self.change}')
            return False

        else:  # is_update()
            if not hasattr(self.change.old_state, key):
                raise Exception

            if getattr(self.change.old_state, key) != \
                    getattr(self.change.new_state, key):
                return True
            else:
                return False


class ChangeTypes(Enum):
    EMPTY = 0
    CREATE = 1
    UPDATE = 2
    DELETE = 3


class Change:
    """An object representing a change in the entity state. It holds the old
     and the new states (as entities) as well as the change type.
    """

    def __init__(self,
                 old_state: Entity = None,
                 new_state: Entity = None,
                 timestamp: str = None,
                 id: str = None):
        """Construct a change object. When the change is of type CREATE or
        DELETE - the old_state or new_state respectively should naturally be
        omitted.

        Raises:
            Exception: Missing id attribute of either entity state.
        """
        self.id = id or get_new_id()

        # This may be done only in debug mode
        if (old_state and not isinstance(old_state, Entity)) or \
                (new_state and not isinstance(new_state, Entity)):
            raise Exception('Changes can only work with Entity sublasses')

        self.old_state = old_state
        self.new_state = new_state

        self.timestamp = timestamp or fusion_timestamp(current_time(),
                                                       microseconds=True)

        self._added = None
        self._removed = None
        self._updated = None

        if not (self.old_state or self.new_state):
            raise ValueError('Both old and new state are None.')

    def __repr__(self) -> str:
        return (f'<Change id={self.id} timestamp={self.timestamp} '
                f'type={self.change_type} '
                f'old_state={self.old_state} new_state={self.new_state}>')

    @property
    def time(self) -> datetime:
        return datetime.fromisoformat(self.timestamp)

    @property
    def added(self) -> Diff:
        if self._added is None:
            self._added = Diff(self, DiffTypes.ADDED)

        return self._added

    @property
    def removed(self) -> Diff:
        if self._removed is None:
            self._removed = Diff(self, DiffTypes.REMOVED)

        return self._removed

    @property
    def updated(self) -> Updated:
        if self._updated is None:
            self._updated = Updated(self)

        return self._updated

    @property
    def change_type(self):
        if self.old_state and self.new_state:
            return ChangeTypes.UPDATE
        elif not self.old_state and not self.new_state:
            return ChangeTypes.EMPTY
        elif not self.old_state:
            return ChangeTypes.CREATE
        else:
            return ChangeTypes.DELETE

    def asdict(self) -> dict:
        if self.old_state:
            old_state = dump_to_dict(self.old_state)
        else:
            old_state = None

        if self.new_state:
            new_state = dump_to_dict(self.new_state)
        else:
            new_state = None

        return dict(old_state=old_state,
                    new_state=new_state,
                    id=self.id,
                    timestamp=self.timestamp)

    @classmethod
    def from_dict(cls, change_dict: dict) -> Change:
        if 'delta' in change_dict:
            return cls.from_safe_delta_dict(change_dict)
        return cls(**change_dict)

    @classmethod
    def from_safe_delta_dict(cls, change_dict: dict) -> Change:
        old_state_dict = change_dict.get('old_state', None)
        new_state_dict = change_dict.get('new_state', None)

        if 'timestamp' not in change_dict:
            raise Exception('Missing timestamp in change dict')

        # Get the delta and use it to generate the new_state
        delta = change_dict.pop('delta', None)
        if delta is not None:
            new_state_dict = copy(old_state_dict)
            new_state_dict.update(**delta)

        if old_state_dict:
            change_dict['old_state'] = load_from_dict(old_state_dict)
        if new_state_dict:
            change_dict['new_state'] = load_from_dict(new_state_dict)

        return cls(**change_dict)

    def as_safe_delta_dict(self):
        self_dict = self.asdict()

        if self.is_update():
            self_dict['delta'] = self.delta()
            del self_dict['new_state']

        return self_dict

    def delta(self):
        delta_dict = {}
        for field in fields(self.old_state):
            old_val = getattr(self.old_state, field.name)
            new_val = getattr(self.new_state, field.name)
            if old_val != new_val:
                delta_dict[field.name] = new_val

        return delta_dict

    @classmethod
    def CREATE(cls, state: Entity) -> Change:
        """Convenience method for constructing a Change with type CREATE"""
        return cls(new_state=copy(state))

    @classmethod
    def UPDATE(cls, old_state: Entity, new_state: Entity) -> Change:
        """Convenience method for constructing a Change with type UPDATE"""
        return cls(old_state=copy(old_state), new_state=copy(new_state))

    @classmethod
    def DELETE(cls, old_state: Entity) -> Change:
        """Convenience method for constructing a Change with type DELETE"""
        return cls(old_state=copy(old_state))

    def is_create(self) -> bool:
        return self.change_type == ChangeTypes.CREATE

    def is_update(self) -> bool:
        return self.change_type == ChangeTypes.UPDATE

    def is_delete(self) -> bool:
        return self.change_type == ChangeTypes.DELETE

    def last_state(self) -> Entity:
        """Returns the latest available state.

        Returns:
            [dict]: If the change is of type UPDATE - returns new_state.
                    Otherwise returns whatever is available (for CREATE -
                    new_state and for DELETE - the old_state)
        """
        if not self.new_state:
            return self.old_state

        return self.new_state

    def reversed(self) -> Change:
        if self.is_create():
            return Change.DELETE(self.new_state)
        elif self.is_delete():
            return Change.CREATE(self.old_state)
        elif self.is_update:
            return Change.UPDATE(self.new_state, self.old_state)


class PDChange(BaseModel):
    id: str
    old_state: dict | None
    new_state: Optional[dict | None] = None
    delta: Optional[dict | None] = None
    timestamp: str
