from typing import Callable, List

import fusion
log = fusion.get_logger(__name__)

_commands = {}


class Command:
    def __init__(self,
                 function: Callable,
                 title: str,
                 name: str):
        self.function = function
        self.title = title
        self.name = name

    def __repr__(self):
        return f'<Command title={self.title}>'

    def __call__(self, **context):
        log.info(f'COMMAND triggered: {self}')
        return self.function(**context)


def command(title: str, name: str = ''):
    def decorator(function: Callable):
        if not name:
            fname = function.__name__
        else:
            fname = name

        _command = Command(function, title, fname)
        _commands[function] = _command
        return _command
    return decorator


def commands() -> List[Command]:
    yield from _commands.values()
