# -*- coding: utf-8 -*-

import logging

LOGGER = logging.getLogger(__name__)


class CallbackMixin(object):
    def __init__(self, *args, **kwargs):
        super(CallbackMixin, self).__init__(*args, **kwargs)
        self._callbacks = {}

    def register_callback(self, event_name, callback):
        """
        Note: if you want to register callbacks with arguments
        register them by wrapping your function using `functools.partial`.
        """
        LOGGER.debug("Registering callback %s for event %s.", callback, event_name)
        self._callbacks.setdefault(event_name, []).append(callback)

    def process_callbacks(self, event_name):
        callbacks = self._callbacks.get(event_name, [])
        LOGGER.debug("Going to process callbacks %s for the event %s.", callbacks, event_name)
        for callback in callbacks:
            callback()
