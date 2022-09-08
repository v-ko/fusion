import importlib.metadata
import random
import time
from typing import Any, Callable

from fusion.loop import MainLoop, NoMainLoop

__version__ = importlib.metadata.version('python-fusion')

from .logging import get_logger
from fusion.libs.entity import entity_type, Entity
from fusion.libs.entity.change import Change, ChangeTypes
# from fusion.state_manager import FusionStateManager
from fusion.loop import main_loop, set_main_loop

line_spacing_in_pixels = 20
_reproducible_ids = False
# class StateManagerWrapper:
#     def __init__(self):
#         self._fsm = FusionStateManager()

#     def __setattr__(self, __name: str, __value: Any) -> None:
#         return setattr(self._fsm, __name, __value)

#     def __getattr__(self, __name: str):
#         return getattr(self._fsm, __name)

# fsm: FusionStateManager = StateManagerWrapper()

# def swap_state_manager(manager: FusionStateManager):
#     fsm._fsm = manager


def call_delayed(callback: Callable,
                 delay: float = 0,
                 args: list = None,
                 kwargs: dict = None):
    """Call a function with a delay on the main loop.

    Args:
        callback (Callable): The callable to be invoked
        delay (float, optional): The delay in seconds. Defaults to 0.
        args (list, optional): A list with the arguments. Defaults to None.
        kwargs (dict, optional): A dictionary with the keyword arguments.
            Defaults to None.
    """
    args = args or []
    kwargs = kwargs or {}

    if not callback:
        raise Exception('Callback cannot be None')

    main_loop().call_delayed(callback, delay, args, kwargs)


# ----------------Various---------------------
def set_reproducible_ids(enabled: bool):
    """When testing - use non-random ids"""
    global _reproducible_ids
    if enabled:
        random.seed(0)
        _reproducible_ids = True
    else:
        random.seed(time.time())
        _reproducible_ids = False


def reproducible_ids():
    return _reproducible_ids
