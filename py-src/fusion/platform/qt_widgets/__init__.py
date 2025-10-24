from typing import TYPE_CHECKING, Any, Callable, Generic, Optional, TypeVar

from PySide6.QtCore import Property as _QtProperty, QObject

from fusion import fsm, set_main_loop
from fusion.libs.entity.change import Change
from fusion.libs.state import ViewState

if TYPE_CHECKING:  # pragma: no cover - typing aid
    T = TypeVar("T")

    class TemplatedProperty(property, Generic[T]):
        fget: Callable[[Any], Any] | None
        fset: Callable[[Any, Any], None] | None
        fdel: Callable[[Any], None] | None
        __isabstractmethod__: bool

        def __init__(
            self,
            fget: Callable[[Any], Any] | None = ...,
            fset: Callable[[Any, Any], None] | None = ...,
            fdel: Callable[[Any], None] | None = ...,
            doc: str | None = ...,
        ) -> None: ...
        def __call__(self, func: Callable[[Any], T], /) -> "TemplatedProperty[T]": ...
        def getter(self, fget: Callable[[Any], Any], /) -> "TemplatedProperty[T]": ...
        def setter(
            self, fset: Callable[[Any, Any], None], /
        ) -> "TemplatedProperty[T]": ...
        def deleter(self, fdel: Callable[[Any], None], /) -> "TemplatedProperty[T]": ...
        def __get__(self, instance: Any, owner: type | None = None, /) -> Any: ...
        def __set__(self, instance: Any, value: Any, /) -> None: ...
        def __delete__(self, instance: Any, /) -> None: ...

    def Property(tp: type[T], *args, **kwargs):
        def deco(fget: Callable[[Any], T]) -> TemplatedProperty[T]:
            return TemplatedProperty[T](fget)

        return deco

else:
    Property = _QtProperty


def bind_and_apply_state(qobject: QObject, state: ViewState, on_state_change: Callable):

    subscription = fsm.state_changes_per_TLA_by_view_id.subscribe(
        on_state_change, index_val=state.view_id
    )
    qobject.destroyed.connect(lambda: subscription.unsubscribe())

    # fusion.call_delayed(on_state_change, args=[Change.CREATE(state)])
    on_state_change(Change.CREATE(state))


def configure_for_qt(app):
    from fusion.platform.qt_widgets.qt_main_loop import QtMainLoop

    set_main_loop(QtMainLoop(app))
