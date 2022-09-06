from copy import copy
import inspect
import json
import time
from pathlib import Path
from contextlib import contextmanager
from PySide6.QtWidgets import QApplication

import fusion
from fusion.basic_classes import Point2D, Rectangle, Color
from fusion.libs.entity import Entity
from fusion.gui.actions_library.action import ActionCall, ActionRunStates
from fusion.gui.misli_gui import on_actions_logged
from fusion.gui.view_library.view_state import ViewState
from pamet import desktop_app


@contextmanager
def exec_action(delay_before_next: float = 0, apply_delay=True):
    t0 = time.time()
    # fusion.main_loop().process_events(repeat=10)
    yield
    # Hacky way to ensure all events get processed
    fusion.main_loop().process_events(repeat=10)

    exec_time = time.time() - t0
    time_left = delay_before_next - exec_time
    if time_left > 0 and apply_delay:
        time.sleep(time_left)


class ActionCamera:

    def __init__(self):
        self.recording = False
        self.TLA_calls = []
        self.classes_used = set()

    def handle_action_call(self, action_call: ActionCall):
        if not action_call.is_top_level or \
                action_call.run_state != ActionRunStates.FINISHED or \
                action_call.issuer != 'user':
            return
        self.TLA_calls.append(action_call)

    def parse_arg(self, arg):
        if isinstance(arg, str):
            return json.dumps(arg)  # Deal with escaping, etc.
        elif isinstance(arg, (bool, int, float)):
            return str(arg)
        elif isinstance(arg, ViewState):
            return f"view_state('{arg.view_id}')"
        elif isinstance(arg, Entity):
            self.classes_used.add(type(arg))
            kwarg_strings = []
            for key, val in arg.asdict().items():
                # key_str = self.parse_arg(key)
                val_str = self.parse_arg(val)
                kwarg_strings.append(f'{key}={val_str}')
            kwargs_str = ', '.join(kwarg_strings)
            return f'{type(arg).__name__}({kwargs_str})'
        elif isinstance(arg, (Point2D, Rectangle, Color)):
            self.classes_used.add(type(arg))
            return f'{type(arg).__name__}{arg.as_tuple()}'
        elif isinstance(arg, dict):
            key_val_strings = []
            for key, val in arg.items():
                key_val_str = f'{self.parse_arg(key)}: {self.parse_arg(val)}'
                key_val_strings.append(key_val_str)
            all_data_str = ', '.join(key_val_strings)
            return '{' + all_data_str + '}'
        elif isinstance(arg, list):
            all_data_str = ', '.join([self.parse_arg(a) for a in arg])
            return '[' + all_data_str + ']'
        elif isinstance(arg, tuple):
            all_data_str = ', '.join([self.parse_arg(a) for a in arg])
            return '(' + all_data_str + ')'
        else:
            raise Exception

    def generate_code_for_action_call(self, action_call: ActionCall):
        func = action_call.function
        func_params = list(inspect.signature(func).parameters.keys())
        kwargs = copy(action_call.kwargs)

        # Convert all to kwargs, so we have better readability
        for i, arg in enumerate(action_call.args):
            kwargs[func_params[i]] = arg
        # arg_strings=
        # args_str = ', '.join([self.parse_arg(arg) for arg in action_call.args])

        kwargs_str = ''
        if kwargs:
            kwarg_strings = []
            for key, val in kwargs.items():
                # key_str = self.parse_arg(key)
                val_str = self.parse_arg(val)
                kwarg_strings.append(f'{key}={val_str}')
            kwargs_str = ', '.join(kwarg_strings)

        # If it's not the last action
        action_call_idx = self.TLA_calls.index(action_call)
        next_idx = action_call_idx + 1

        # Get the time of the next call. If at the last - get the current
        # time (i.e. the time of closing)
        if next_idx < len(self.TLA_calls):
            next_start_time = self.TLA_calls[next_idx].start_time
        else:
            next_start_time = time.time()

        delay_before_next = (next_start_time - action_call.start_time +
                             action_call.duration)
        delay_before_next_str = f'delay_before_next={delay_before_next}'
        exec_kwargs_str = delay_before_next_str
        exec_kwargs_str += ', apply_delay=not run_headless'
        action_call_code = (
            f'    with exec_action({exec_kwargs_str}):\n'
            f'        {func.__module__}.{func.__name__}({kwargs_str})')
        return action_call_code

    def generate_code_for_recording(self, *args):
        action_call_code_chunks = []
        for action_call in self.TLA_calls:
            action_call_code = self.generate_code_for_action_call(action_call)
            action_call_code_chunks.append(action_call_code)

        import_strings = []
        for class_used in self.classes_used:
            import_strings.append(
                f'from {class_used.__module__} import {class_used.__name__}')

        imports_str = "\n".join(import_strings)
        code_str = '\n\n'.join(action_call_code_chunks)
        code_for_recording = f'''import pamet
from fusion.visual_inspection.action_camera import exec_action
from fusion.gui.misli_gui import view_state

{imports_str}


def test_new(window_fixture, request):
    run_headless = request.config.getoption('--headless')

{code_str}

    assert True
        '''

        return code_for_recording

    def record_until_exit(self, path: Path):
        sub = on_actions_logged(self.handle_action_call)
        desktop_app.get_app().aboutToQuit.connect(
            lambda: QApplication.clipboard().setText('TestTest'))
        desktop_app.get_app().exec()
        sub.unsubscribe()
        path.write_text(self.generate_code_for_recording())
