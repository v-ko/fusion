from contextlib import contextmanager
import time
import functools
from typing import Callable

import fusion
from fusion.libs.action.action_call import ActionCall, ActionRunStates
from fusion.libs.channel import Channel, Subscription
from fusion.logging import BColors

log = fusion.get_logger(__name__)

_actions_by_name = {}
_names_by_wrapped_func = {}
_names_by_unwrapped_func = {}
_unwrapped_action_funcs_by_name = {}

completed_root_actions = Channel('__COMPLETED_ROOT_ACTIONS__')
actions_log_channel = Channel('__ACTIONS_LOG__')

_action_context_stack = []

_view_and_parent_update_ongoing = False


def unwrapped_action_by_name(action_name: str):
    return _unwrapped_action_funcs_by_name[action_name]


def name_for_wrapped_action(action_function: Callable):
    return _names_by_wrapped_func[action_function]


def wrapped_action_by_name(name: str) -> Callable:
    return _actions_by_name[name]


@contextmanager
def lock_actions():
    global _view_and_parent_update_ongoing
    _view_and_parent_update_ongoing = True
    yield None
    _view_and_parent_update_ongoing = False


def view_and_parent_update_ongoing():
    return _view_and_parent_update_ongoing


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


def execute_action(_action):
    _action.run_state = ActionRunStates.STARTED
    log_action_call(_action)

    # We get an action context (i.e. push this action on the stack)
    # Mainly in order to handle action nesting and do view updates
    # only after the completion of the top-level(=root) action.
    # That way redundant GUI rendering is avoided inside an action that
    # makes multiple update_state calls and/or invokes other actions
    _action.is_top_level = not is_in_action()
    with action_context(_action):
        # Call the actual function
        return_val = _action.function(*_action.args, **_action.kwargs)

    _action.duration = time.time() - _action.start_time
    _action.run_state = ActionRunStates.FINISHED
    log_action_call(_action)

    return return_val


def action(name: str, issuer: str = 'user'):
    """A decorator that adds an action state emission on the start and end of
    each function call (via fusion.gui.push_action).

    On module initialization this decorator saves the decorated function in the
     actions library (by name). By registering actions and providing a stream
     of action states it's possible to later replay the app usage.

    Args:
        name (str): The name of the action (domain-like naming convention, e.g.
         'context.action_name')
    """
    if not name or not isinstance(name, str):
        raise Exception(
            'Please add the action name as an argument to the decorator. '
            'E.g. @action(\'action_name\')')

    def decorator_action(func):

        @functools.wraps(func)
        def wrapper_action(*args, **kwargs):
            _action = ActionCall(name,
                                 issuer=issuer,
                                 args=list(args),
                                 kwargs=kwargs)

            if fusion.libs.action.view_and_parent_update_ongoing():
                raise Exception(
                    'Cannot invoke an action while updating the views.')
                #   f' Queueing {_action} on the main loop.')
                # gui.actions_queue_channel.push(_action)
                # return

            return execute_action(_action)

        if name in _unwrapped_action_funcs_by_name:
            raise Exception(f'An action with the name {name} is already'
                            f' registered')

        _actions_by_name[name] = wrapper_action
        _names_by_wrapped_func[wrapper_action] = name
        _names_by_unwrapped_func[func] = name
        _unwrapped_action_funcs_by_name[name] = func

        return wrapper_action

    return decorator_action
