from time import sleep
from typing import Callable
from PySide6.QtCore import QEventLoop, QTimer
from PySide6.QtWidgets import QApplication
from fusion.loop import MainLoop


class QtMainLoop(MainLoop):

    def __init__(self, app: QApplication):
        self.app = app
        self.queue_checksum = 0

    def call_delayed(self,
                     callback: Callable,
                     delay: float = 0,
                     args: list = None,
                     kwargs: dict = None):

        args = args or []
        kwargs = kwargs or {}

        if not isinstance(callback, Callable):
            raise Exception

        def report_and_callback():
            self.queue_checksum -= 1
            callback(*args, **kwargs)

        self.queue_checksum += 1

        # lambda: callback(*args, **kwargs))
        QTimer.singleShot(delay * 1000, report_and_callback)

    def process_events(self, repeat: int = 0):
        # A hacky way to be sure that all posted events are called
        self.app.processEvents()  # QEventLoop.WaitForMoreEvents
        self.app.sendPostedEvents()
        if self.queue_checksum:
            self.process_events()

        if repeat:
            sleep(0.001)
            self.process_events(repeat=repeat - 1)

    def loop(self):
        self.app.exec()
