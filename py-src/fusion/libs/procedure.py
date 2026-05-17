"""The ``@procedure`` decorator — async counterpart to ``@action``.

Actions are synchronous functions that mutate state on the Qt main thread.
Procedures are async functions that orchestrate work (I/O, delays, waiting
for results) — also on the Qt main thread.

    @action('update_overlay')
    def update_overlay(shapes): ...      # sync, immediate

    @procedure
    async def run_experiment():           # async, scheduled as a Task
        shapes = await detect_shapes()
        update_overlay(shapes)

Calling a ``@procedure``-decorated function immediately schedules the
coroutine on the running asyncio event loop and returns the ``Task``.
The task is already started — no explicit ``create_task`` needed.
"""

from __future__ import annotations

import asyncio
import functools
import inspect

from fusion.logging import get_logger

log = get_logger(__name__)


def _log_task_exception(task: asyncio.Task) -> None:
    """Done-callback that logs unhandled exceptions from procedure tasks."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error(
            "Unhandled exception in procedure '%s'",
            task.get_name(),
            exc_info=(type(exc), exc, exc.__traceback__),
        )


def procedure(fn):
    """Decorator: auto-schedule an async function as an asyncio Task.

    When called, the coroutine is immediately scheduled on the running
    event loop and the Task is returned.  Can be awaited from async code
    or used fire-and-forget from synchronous Qt slots.

    Unhandled exceptions are logged automatically.
    """
    if not inspect.iscoroutinefunction(fn):
        raise TypeError(f"@procedure can only decorate async functions, got {fn!r}")

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        coro = fn(*args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "No running asyncio event loop. "
                "Ensure fusion.platform.qt_widgets.qt_event_loop.install() "
                "has been called."
            ) from None
        task = loop.create_task(coro, name=fn.__qualname__)
        task.add_done_callback(_log_task_exception)
        return task

    wrapper._is_procedure = True
    return wrapper
