"""It supports adding named channels on which to subscribe handlers and then
dispatch messages (which are arbitrary python objects).

Dispatching and invoking the handlers are both done on the same thread, so
it's expected that the subscribed callables are light, since the main purpose
of fusion is GUI rendering and blocking the main loop would cause freezing.
"""

from typing import Callable, Dict, Any
from collections import defaultdict
from enum import Enum
from dataclasses import MISSING

import fusion
from fusion import get_logger

log = get_logger(__name__)

_channels = {}


def unsibscribe_all():
    for channel_name, channel in _channels.items():
        for sub_props, sub in list(channel.subscriptions.items()):
            sub.unsubscribe()


class SubscriptionTypes(Enum):
    CHANNEL = 1
    ENTITY = 2
    INVALID = 0


class Subscription:

    def __init__(self, handler, channel, index_val: Any = MISSING):
        self.id = id(self)
        self.handler = handler
        self.channel = channel
        self.index_val = index_val

    def props(self):
        return self.handler, self.channel, self.index_val

    def unsubscribe(self):
        self.channel.remove_subscribtion(self)


class Channel:

    def __init__(self,
                 name: str,
                 index_key: Callable = None,
                 filter_key: Callable = None):
        self.name = name
        self.index_key = index_key
        self.filter_key = filter_key
        # self.message_stack = []
        self.subscriptions: Dict[tuple, Subscription] = {}  # by id

        # if index_key:
        self.index = defaultdict(list)

        if name in _channels:
            raise Exception('A channel with this name already exists')
        _channels[name] = self

    def __repr__(self):
        return f'<Channel name={self.name}>'

    @log.traced
    def push(self, message):
        if self.filter_key:
            if not self.filter_key(message):
                return

        # self.message_stack.append(message)
        # call_delayed(self.notify_subscribers)
        # !!! NO, this way messages get batched by channel and
        # the order is lost

        # # log.info('^^PUSH^^ on "%s": %s' % (self.name, message))

        for sub_props, sub in self.subscriptions.items():
            if self.index_key and sub.index_val is not MISSING:
                if self.index_key(message) != sub.index_val:
                    continue

            log.info(f'Queueing {sub.handler=} for {message=} on'
                     f' channel_name={self.name}')
            fusion.call_delayed(sub.handler, 0, args=[message])

    def subscribe(self, handler: Callable, index_val: Any = MISSING):
        sub = Subscription(handler, _channels[self.name], index_val)
        self.add_subscribtion(sub)
        return sub

    def add_subscribtion(self, subscribtion):
        if subscribtion.props() in self.subscriptions:
            raise Exception(f'Subscription with props {subscribtion.props()} '
                            f'already added to channel '
                            f'{self.name}')

        self.subscriptions[subscribtion.props()] = subscribtion

    def remove_subscribtion(self, subscribtion):
        if subscribtion.props() not in self.subscriptions:
            raise Exception(
                f'Cannot unsubscribe missing subscription with props'
                f' {subscribtion.props()}'
                f' in channel {self.name}')

        self.subscriptions.pop(subscribtion.props())

    # def notify_subscribers(self):
    # !!! NO, this way messages get batched by channel and the order is lost
    #     if not self.message_stack:
    #         return

    #     # # Iterate over a copy of the subscriptions, since an additional handler
    #     # # can get added while executing the present handlers
    #     # for sub_props, sub in copy(self.subscriptions).items():
    #     #     # If the channel is not indexed or the subscriber does not filter
    #     #     # messages using the index - notify for all messages
    #     #     if not self.index_key or sub.filter_val is MISSING:
    #     #         messages = self.message_stack
    #     #     else:
    #     #         messages = self.index.get(sub.filter_val, [])

    #     #     for message in messages:
    #     #         log.info(f'Calling {sub.handler=} for {message=} on'
    #     #                  f' channel_name={sub.channel_name}')
    #     #         sub.handler(message)

    #     for message in copy(self.message_stack):
    #         # Copy the subscriptions we iterate over, because the list can
    #         # change while executing the actions and we don't want that
    #         for sub_props, sub in copy(self.subscriptions).items():
    #             if self.index_key and sub.index_val is not MISSING:
    #                 if self.index_key(message) != sub.index_val:
    #                     continue

    #             log.info(f'Calling {sub.handler=} for {message=} on'
    #                      f' channel_name={self.name}')
    #             call_delayed(sub.handler, 0, args=[message])
    #             # !!!! sub.handler(message)
    #     self.message_stack.clear()
    #     # self.index.clear()
