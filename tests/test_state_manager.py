from dataclasses import field
import fusion
from fusion import fsm
from fusion.libs import action as actions_lib
from fusion.libs.action import action, wrapped_action_by_name
from fusion.libs.action.action_call import ActionCall
from fusion.libs.state import ViewState, view_state_type
from fusion.loop import NoMainLoop


def test_view_state_updates_and_diffing():
    main_loop = NoMainLoop()
    fusion.set_main_loop(main_loop)

    expected_raw_state_changes = []

    children = []
    children_left = []

    @view_state_type
    class MockViewState(ViewState):
        test_field: str = 'not_set'
        child_states: set = field(default_factory=set)

    @action('dummy_nested')
    def dummy_nested():
        pass

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

    @action('remove_child')
    def remove_child(parent_state):
        removed_child = parent_state.child_states.pop()
        children_left.extend(list(parent_state.child_states))
        change = fsm.remove_state(removed_child)
        expected_raw_state_changes.append(change)

        change = fsm.update_state(parent_state)
        expected_raw_state_changes.append(change)
        return removed_child

    expected_top_level_action_functions = [create_view_state, remove_child]
    completed_root_level_action_functions = []
    received_raw_state_changes = []

    def handle_completed_root_actions(action: ActionCall):
        completed_root_level_action_functions.append(
            wrapped_action_by_name(action.name))

    def handle_raw_state_changes(change):
        received_raw_state_changes.append(change)

    def handle_state_change(change):
        state: MockViewState = change.last_state()
        assert change.updated.test_field
        assert state.test_field == 'set'

        assert set(children) == set(change.added.child_states)

    actions_lib.completed_root_actions.subscribe(handle_completed_root_actions)
    fsm.raw_state_changes.subscribe(handle_raw_state_changes)

    parent_state = create_view_state()
    subscription = fsm.state_changes_per_TLA_by_view_id.subscribe(
        handle_state_change, index_val=parent_state.id)

    main_loop.process_events()
    removed_child = remove_child(parent_state)

    def handle_state_change(change):
        state: MockViewState = change.last_state()
        assert set([removed_child]) == \
            set(change.removed.child_states)
        assert state.child_states == set(children_left)

    subscription.unsubscribe()
    fsm.state_changes_per_TLA_by_view_id.subscribe(handle_state_change,
                                                   index_val=parent_state.id)
    main_loop.process_events()

    assert completed_root_level_action_functions == \
        expected_top_level_action_functions
