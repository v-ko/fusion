"""The fusion.gui module provides basic functionality for handling GUI state
and updates. The main parts of the fusion based GUI app are Views, their
respective View Models, and Actions (alternatively called usecases).

View classes are widgets (in Qt) or e.g. React components in web apps.
Though they are allowed to have a local state - all properties that can be
changed by other classes or functions should be in their View Model.

Each instance of a view and their view model get registered (automatically)
    upon creation in fusion.gui. And the only proper way to change a View
    Model is via fusion.update_view_model(new_model). That method chaches
    the updated models and in that way unneeded GUI renderings are avoided
    in the midst of complex user actions. After all action logic is executed -
    the updated view models are pushed to the views (by invoking their
    View.handle_model_update virtual method).

Actions are simple functions that should carry the bulk of the GUI
interaction logic. They should be decorated with fusion.gui.actions_lib.action
in order to have proper logging and reproducability of the user
interactions. E.g. if we want to change the background of a View, we'll
create an action like:

@action('change_background')
def change_background(view_id, background_color):
    view_model = fusion.gui.view_model(view_id)
    view_model.background_color = background_color
    fusion.update_view_model(view_model)

After calling this action - the View.handle_model_update will be invoked
with the new model. This allows for complex GUI logic that is reproducible
and enforces the avoidance of endless nested callbacks.

TODO: Example on a simple View + View Model
"""
from typing import Callable
import time
import random
from contextlib import contextmanager

import fusion
from fusion import Change
from fusion.change_aggregator import ChangeAggregator
from fusion.platform.base_provider import BaseUtilitiesProvider
from fusion.logging import BColors
from fusion.libs.channel import Channel, Subscription

from fusion.libs.action import ActionCall, execute_action
from fusion.libs.action import name_for_wrapped_action
from fusion.view import ViewState

log = fusion.get_logger(__name__)

raw_state_changes = Channel('__RAW_STATE_CHANGES__')
state_changes_per_TLA_by_view_id = Channel(
    '__AGGREGATED_STATE_CHANGES_PER_TLA__', lambda x: x.last_state().view_id)
completed_root_actions = Channel('__COMPLETED_ROOT_ACTIONS__')

actions_queue_channel = Channel('__ACTIONS_QUEUE__')
actions_log_channel = Channel('__ACTIONS_LOG__')

# def execute_action_queue(actions: List[Action]):
#     for action in actions:
#         execute_action(action)

actions_queue_channel.subscribe(execute_action)

_state_aggregator = ChangeAggregator(
    input_channel=raw_state_changes,
    release_trigger_channel=completed_root_actions,
    output_channel=state_changes_per_TLA_by_view_id)

_view_states = {}
_state_backups = {}

_action_context_stack = []
_view_and_parent_update_ongoing = False

_util_provider: BaseUtilitiesProvider = None


def util_provider() -> BaseUtilitiesProvider:
    return _util_provider


def set_util_provider(provider: BaseUtilitiesProvider):
    global _util_provider
    _util_provider = provider


def view_and_parent_update_ongoing():
    return _view_and_parent_update_ongoing


@contextmanager
def lock_actions():
    global _view_and_parent_update_ongoing
    _view_and_parent_update_ongoing = True
    yield None
    _view_and_parent_update_ongoing = False


@contextmanager
def action_context(action):
    _action_context_stack.append(action)
    yield None

    # If it's a root action - propagate the state changes to the views (async)
    _action_context_stack.pop()
    if not _action_context_stack:
        completed_root_actions.push(action)


def is_in_action():
    return bool(_action_context_stack)


def ensure_context():
    if not is_in_action():
        raise Exception(
            'State changes can only happen in functions decorated with the '
            'fusion.gui.action.action decorator')


# Action channel interface
def log_action_call(action_call: ActionCall):
    """Push an action to the actions channel and handle logging. Should only be
    called by the action decorator.
    """
    args_str = ', '.join([str(a) for a in action_call.args])
    kwargs_str = ', '.join(
        ['%s=%s' % (k, v) for k, v in action_call.kwargs.items()])

    indent = '.' * 4 * (len(_action_context_stack) - 1)

    green = BColors.OKGREEN
    end = BColors.ENDC
    msg = (f'{indent}Action {green}{action_call.run_state.name} '
           f'{action_call.name}{end} '
           f'ARGS=*({args_str}) KWARGS=**{{{kwargs_str}}}')
    if action_call.duration != -1:
        msg += f' time={action_call.duration * 1000:.2f}ms'
    log.info(msg)

    actions_log_channel.push(action_call.copy())


@log.traced
def on_actions_logged(handler: Callable) -> Subscription:
    """Register a callback to the actions channel. It will be called before and
    after each action call. It's used for user interaction recording.

    Args:
        handler (Callable): The callable to be invoked on each new message on
        the channel
    """
    return actions_log_channel.subscribe(handler)


def queue_action(action_func, args: list = None, kwargs: dict = None):
    action = ActionCall(name_for_wrapped_action(action_func))
    action.args = args or []
    action.kwargs = kwargs or {}
    actions_queue_channel.push(action)


@log.traced
def add_state(state_: ViewState):
    ensure_context()
    if state_.view_id in _view_states:
        raise Exception(
            f'View state with id {state_.view_id} already present.')
    state_._added = True
    _view_states[state_.view_id] = state_
    _state_backups[state_.view_id] = state_.copy()
    change = Change.CREATE(state_)
    raw_state_changes.push(change)
    return change


def view_state_exists(view_id: str) -> bool:
    return view_id in _view_states


def view_state(view_id):
    return _view_states[view_id]


def get_state_backup(view_id: str):
    return _state_backups[view_id]


@log.traced
def update_state(state_: ViewState):
    ensure_context()
    if state_.view_id not in _view_states:
        raise Exception('Cannot update a state which has not been added.')

    change = Change.UPDATE(_state_backups[state_.view_id], state_)
    raw_state_changes.push(change)

    if (state_._version + 1) <= _view_states[state_.view_id]._version:
        raise Exception('You\'re using an old state. This object has already '
                        'been updated')

    _state_backups[state_.view_id] = state_.copy()
    state_._version += 1
    _view_states[state_.view_id] = state_
    return change


@log.traced
def remove_state(state_: ViewState):
    ensure_context()
    state_ = _view_states.pop(state_.view_id)
    change = Change.DELETE(state_)
    raw_state_changes.push(change)
    return change


# ----------------Various---------------------
def set_reproducible_ids(enabled: bool):
    """When testing - use non-random ids"""
    if enabled:
        random.seed(0)
    else:
        random.seed(time.time())
