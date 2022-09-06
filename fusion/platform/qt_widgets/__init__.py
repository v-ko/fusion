from typing import Callable
from PySide6.QtCore import QObject
from fusion import fsm, set_main_loop
from fusion.libs.entity.change import Change
from fusion.libs.state import ViewState


def bind_and_apply_state(qobject: QObject, state: ViewState,
                         on_state_change: Callable):

    subscription = fsm.state_changes_per_TLA_by_view_id.subscribe(
        on_state_change, index_val=state.view_id)
    qobject.destroyed.connect(lambda: subscription.unsubscribe())

    # fusion.call_delayed(on_state_change, args=[Change.CREATE(state)])
    on_state_change(Change.CREATE(state))


def configure_for_qt(app):
    from fusion.platform.qt_widgets.qt_main_loop import QtMainLoop

    set_main_loop(QtMainLoop(app))
