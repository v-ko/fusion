"""Fusion is a state manager (fsm) and a helper library for building GUIs in
Python.

Changes to the state should be made only in functions decorated as actions.
On the completion of every root action the updates to the view states get
sent to the views (via the state_changes_per_TLA_by_view_id channel).

Example usage:
```
@view_state_type
class MockViewState(ViewState):
    test_field: str = 'not_set'
    child_states: set = field(default_factory=set)

@action('add_children')  # Is called nested in create_view_state
def add_children(parent_state: MockViewState):
    parent_state.test_field = 'set'
    for i in range(3):
        child = MockViewState()

        parent_state.child_states.add(child)
        children.append(child)

        change = fsm.add_state(child)
        expected_raw_state_changes.append(change)
    change = fsm.update_state(parent_state)
    expected_raw_state_changes.append(change)

@action('create_view_state')
def create_view_state():
    parent_state = MockViewState()
    change = fsm.add_state(parent_state)
    expected_raw_state_changes.append(change)
    add_children(parent_state)
    dummy_nested()
    return parent_state

def handle_state_change(change):
    state: MockViewState = change.last_state()
    assert change.updated.test_field
    assert state.test_field == 'set'

    assert set(children) == set(change.added.child_states)

parent_state = create_view_state()
subscription = fsm.state_changes_per_TLA_by_view_id.subscribe(
    handle_state_change, index_val=parent_state.id)
```
"""
import fusion
from fusion.libs.entity.change import Change
from fusion.change_aggregator import ChangeAggregator
from fusion.libs.channel import Channel
from fusion.libs.action import completed_root_actions, ensure_context, execute_action
from fusion.libs.state import ViewState

log = fusion.get_logger(__name__)

raw_state_changes = Channel('__RAW_STATE_CHANGES__')
state_changes_per_TLA_by_view_id = Channel(
    '__AGGREGATED_STATE_CHANGES_PER_TLA__', lambda x: x.last_state().view_id)

actions_queue_channel = Channel('__ACTIONS_QUEUE__')
actions_queue_channel.subscribe(execute_action)

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
