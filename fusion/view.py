from __future__ import annotations
from multiprocessing.util import get_logger

from fusion import fsm, get_logger
from fusion.libs.state import ViewState

log = get_logger(__name__)


class View:
    """This base View class should be inherited by all view implementations in
    a Misli app.
    """

    def __init__(self, initial_state: ViewState = None):
        if not initial_state:
            initial_state = ViewState()
        self._view_id = initial_state.view_id

    def __repr__(self):
        return '<%s view_id=%s>' % (type(self).__name__, self.view_id)

    @property
    def view_id(self):
        return self._view_id

    def state(self) -> ViewState:
        if fsm.view_state_exists(self._view_id):
            self_state = fsm.view_state(self._view_id)
        else:
            self_state = fsm.get_state_backup(self._view_id)
        return self_state
