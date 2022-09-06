from __future__ import annotations
from dataclasses import field
from typing import Any

from fusion import entity_type, fsm
from fusion.libs.action import is_in_action
from fusion.libs.entity import Entity
from fusion.logging import LOGGING_LEVEL, LoggingLevels


def __hash__(self) -> int:
    return hash(self.view_id)


def view_state_type(view_state_class: Any):
    view_state_class = entity_type(view_state_class, repr=False)
    # Transplant the __hash__ because the dataclasses lib ignores it if it's
    # inherited from a superclass.
    view_state_class.__hash__ = __hash__
    return view_state_class


@view_state_type
class ViewState(Entity):
    '''Mind putting this class as last inherited when also inheriting from
    an Entity with a custom id field, so the latter does not get overwritten'''
    view_id: str = field(default_factory=lambda: fsm.get_view_id())
    _added: bool = field(default=False, init=False, repr=False)
    _version: int = field(default=0, init=False, repr=False)

    def __setattr__(self, key, value):
        # Do thorough checks only when debugging
        if LOGGING_LEVEL != LoggingLevels.DEBUG.value:
            return object.__setattr__(self, key, value)

        if self._added and not is_in_action():
            raise Exception('View states can be modified only in actions')

        # Allow setting the view id only on init
        # It must be immutable, since view states are hashed by it
        if key == 'view_id' and hasattr(self, key):
            raise Exception('view_id is immutable (it\' used for hashing)')

        Entity.__setattr__(self, key, value)

    def __repr__(self) -> str:
        return (f'<{type(self).__name__} id={self.id}'
                f'view_id={self.view_id}>')
