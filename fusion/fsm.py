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
import fusion
from fusion.libs.entity.change import Change
from fusion.change_aggregator import ChangeAggregator
from fusion.libs.channel import Channel
from fusion.libs.action import completed_root_actions, ensure_context
from fusion.libs.state import ViewState

log = fusion.get_logger(__name__)

raw_state_changes = Channel('__RAW_STATE_CHANGES__')
state_changes_per_TLA_by_view_id = Channel(
    '__AGGREGATED_STATE_CHANGES_PER_TLA__', lambda x: x.last_state().view_id)

_state_aggregator = None

_view_states = {}
_state_backups = {}

_last_view_id = 0


def setup():
    global _state_aggregator
    _state_aggregator = ChangeAggregator(
        input_channel=raw_state_changes,
        release_trigger_channel=completed_root_actions,
        output_channel=state_changes_per_TLA_by_view_id)


setup()


def reset():
    global _last_view_id
    _view_states.clear()
    _state_backups.clear()
    _last_view_id = 0
    setup()


def get_view_id():
    global _last_view_id
    _last_view_id += 1
    return str(_last_view_id)


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
        raise Exception('You\'re using an old state. This object has '
                        'already been updated')

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
