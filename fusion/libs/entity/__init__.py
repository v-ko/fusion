from __future__ import annotations
import json
from datetime import datetime
from typing import Any, Union
from dataclasses import dataclass, field, fields
import fusion

from fusion.logging import get_logger

from fusion.logging import LOGGING_LEVEL, LoggingLevels

from fusion.util import get_new_id

log = get_logger(__name__)
entity_library = {}


_last_entity_id = 0


def get_entity_id():
    global _last_entity_id
    if fusion.reproducible_ids():
        _last_entity_id += 1
        return str(_last_entity_id).zfill(8)
    else:
        return get_new_id()


def reset_entity_id_counter():
    global _last_entity_id
    _last_entity_id = 0
    # For debugging purposes


def __hash__(self):
    return hash(self.gid())


def __eq__(self, other: Entity) -> bool:
    if not other:
        return False
    return self.gid() == other.gid()


def entity_type(entity_class: Any, repr: bool = False):
    """A class decorator to register entities in the entity library for the
    purposes of serialization and deserialization. It applies the dataclass
    decorator.
    """
    # entity_class = _apply_dataclass_and_process_properties(entity_class)

    # Transplant __hash__ and __eq__ into every entity upon registration
    # because the dataclasses lib disregards the inherited ones
    entity_class.__hash__ = __hash__
    entity_class.__eq__ = __eq__

    if hasattr(entity_class, 'type_name'):
        raise Exception('The type_name identifier is used in the '
                        'serialization and is prohibited.')

    entity_class = dataclass(entity_class, repr=repr)

    # Register the entity class
    entity_class_name = entity_class.__name__
    if entity_class_name in entity_library:
        raise Exception('This entity class name is already registered: %s' %
                        entity_class_name)

    entity_library[entity_class_name] = entity_class
    return entity_class


def get_entity_class_by_name(entity_class_name: str):
    return entity_library[entity_class_name]


def dump_to_dict(entity: Entity) -> dict:
    entity_dict = entity.asdict()

    if 'type_name' in entity_dict:
        raise Exception

    type_name = type(entity).__name__
    entity_dict['type_name'] = type_name
    return entity_dict


def dump_as_json(entity: Entity, ensure_ascii=False, **dump_kwargs):
    entity_dict = dump_to_dict(entity)
    json_str = json.dumps(entity_dict, ensure_ascii=False, **dump_kwargs)
    return json_str


def load_from_json(json_str: str):
    raise Exception('not tested')
    entity_dict = json.loads(json_str)
    load_from_dict(entity_dict)


def load_from_dict(entity_dict: dict):
    type_name = entity_dict.pop('type_name')
    cls = get_entity_class_by_name(type_name)

    if 'id' in entity_dict:
        id = entity_dict.pop('id')
        if isinstance(id, list):  # Mostly when deserializing
            id = tuple(id)
        instance = cls(id=id)
    else:
        instance = cls()

    leftovers = instance.replace_silent(**entity_dict)
    if leftovers:
        log.error(f'Leftovers while loading entity '
                  f'(id={entity_dict.get("id", None)}): {leftovers}')
    return instance


@entity_type
class Entity:
    """The base class for entities. Provides several convenience methods for
    conversions to and from dict, copying and attribute updates (via replace())

    All entity subclasses should be decorated with register_entity_type like so:

        from fusion.entity import register_entity_type

        @entity_type
        class EntitySubclass:
            ...

    Dataclasses provides a nice syntax for attribute declaration (check out
    the python documentation), while the register_entity_type decorator
    registers the new subclass for the purposes of serialization.

    The __init__ is used by dataclass, so in order to do stuff upon
    construction you need to reimplement __post_init__.

    If you want to use computed properties declared via @property you should
    declare the dataclass attributes with a leading underscore and the methods
    without it. See the register_entity_type docs for an example.
    """

    id: str | tuple = field(default_factory=get_entity_id)
    immutability_error_message: str = field(default=False,
                                            init=False,
                                            repr=False)

    def __setattr__(self, key, value):
        # Do thorough checks only when debugging
        if LOGGING_LEVEL != LoggingLevels.DEBUG.value:
            return object.__setattr__(self, key, value)

        if self.immutability_error_message and \
                key != 'immutability_error_message':
            raise Exception(self.immutability_error_message)

        if isinstance(value, datetime):
            if not value.tzinfo:
                raise Exception

        field_names = [f.name for f in fields(self)]
        if not hasattr(self, key) and key not in field_names:
            raise Exception('Cannot set missing attribute')

        # Since ids are used for hashing - it's wise to make them immutable
        if key == 'id':
            assert isinstance(value, (str, tuple))  # Allowed types for id

            if hasattr(self, 'id'):
                raise Exception

        return object.__setattr__(self, key, value)

    def __post_init__(self):
        # Just for consistency if a subclass implements it at some point
        # removes it again - sub-sub-classes
        # who implement __post_init__ won't need to change
        # (add/remove the super().__post_init__() calls)
        pass

    # vv These two get transplanted in the entity_type decorator, since
    # the dataclasses lib disregards them in child classes
    # def __hash__(self):
    #     return hash(self.gid())

    # def __eq__(self, other: 'Entity') -> bool:
    #     if not other:
    #         return False
    #     return self.gid() == other.gid()

    def __repr__(self) -> str:
        return f'<{type(self).__name__} id={self.id}>'

    def __copy__(self):
        return self.copy()

    @classmethod
    def create_silent(cls, **props):
        if 'id' in props:
            entity = cls(id=props.pop('id'))
        else:
            entity = cls()
        entity.replace_silent(**props)

        return entity

    def copy(self) -> 'Entity':
        self_copy = type(self)(**self.asdict())
        return self_copy

    def with_id(self, new_id: str) -> Entity:
        """A convinience method to produce a copy with a changed id (since
        the 'id' attribute is immutable (used in hashing))."""
        self_dict = self.asdict()
        self_dict['id'] = new_id
        return type(self)(**self_dict)

    def gid(self) -> Union[str, tuple]:
        """Returns the global id of the entity. This function can be
        overwritten to return e.g. a tuple of values like
        (self.page_id, self.id)
        """
        return self.id

    def asdict(self) -> dict:
        """Return the entity fields as a dict"""
        # The dataclasses.asdict recurses and that's not what we want
        self_dict = {
            f.name: getattr(self, f.name)
            for f in fields(self) if f.repr
        }

        for key, val in self_dict.items():
            if isinstance(val, (list, dict, set)):
                val = val.copy()
                self_dict[key] = val

        return self_dict

    def replace(self, **changes):
        """Update entity fields using keyword arguments"""
        for key, val in changes.items():
            setattr(self, key, val)

    def replace_silent(self, **changes):
        """Same as replace, but ignores fields that are not present in the
        dataclass."""
        if 'id' in changes:
            id = changes.pop('id')
            if id != self.id:
                raise Exception(
                    'The id of an entity is immutable.'
                    'To produce a copy with a changed id use Entity.with_id')

        leftovers = {}
        for key, val in changes.items():
            if not hasattr(self, key):
                leftovers[key] = val
                continue
            setattr(self, key, val)
        return leftovers

    def parent_gid(self):
        """Implement this to return the parent global id
        """
        return None

    def set_immutable(self,
                      immutable: bool = True,
                      error_message: str = 'Entity marked as immutable.'):
        """!! Works only in debugging mode (LOGLEVEL=DEBUG in env)!!
        Marks the entity as immutable and an exception with the given
        error_message is raised if __setattr__ is called."""
        if not immutable:
            self.immutability_error_message = ''
            return

        self.immutability_error_message = error_message
