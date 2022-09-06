from __future__ import annotations

import fusion
from .view_state import ViewState

log = fusion.get_logger(__name__)


class View:
    """This base View class should be inherited by all view implementations in
    a Misli app.
    """
    def __init__(self,
                 initial_state: ViewState = None):
        if not initial_state:
            initial_state = ViewState()
        self._view_id = initial_state.view_id

    def __repr__(self):
        return '<%s view_id=%s>' % (type(self).__name__, self.view_id)

    @property
    def view_id(self):
        return self._view_id

    def state(self) -> ViewState:
        if fusion.gui.view_state_exists(self._view_id):
            self_state = fusion.gui.view_state(self._view_id)
        else:
            self_state = fusion.gui.get_state_backup(self._view_id)
        return self_state
