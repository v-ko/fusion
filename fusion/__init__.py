from .logging import get_logger
from fusion.entity_library import entity_type
from fusion.entity_library.entity import Entity
from fusion.entity_library.change import Change, ChangeTypes
from .pubsub import set_main_loop, main_loop, call_delayed
from .pubsub import SubscriptionTypes, Subscription, Channel
from . import gui


line_spacing_in_pixels = 20


def configure_for_qt(app):
    from fusion.gui.utils.qt_widgets.qt_main_loop import QtMainLoop
    from fusion.gui.utils.qt_widgets.provider import QtWidgetsUtilProvider
    from fusion.gui.views.context_menu.widget import ContextMenuWidget
    from fusion.gui.views.input_modal.widget import InputDialogWidget
    from fusion.gui.views.message_box.widget import MessageBoxWidget

    set_main_loop(QtMainLoop(app))