import time
from functools import partial
from threading import Thread

from .support import support
from .utils import chunkify


__all__ = (
    'Worker',
)


class Worker(Thread):

    def __init__(self, queue, executor, bulk_size=1, polling_time=0):
        self._queue = queue
        self._executor = executor
        self._bulk_size = bulk_size
        self._polling_time = polling_time

    def run(self):
        try:
            while True:
                messages = self._queue.get()

                for chunk in chunkify(messages, self._bulk_size):
                    future = self._executor.submit(self._queue.handler, chunk)
                    future.add_done_callback(partial(self._task_done, chunk=chunk))

                if self._shutdown:
                    return

                time.sleep(self._polling_time)

        except Exception as exc:
            support.logger.error(f'Oops! Worker is failed: {repr(e)}', exc_info=exc)

    def shutdown(self):
        self._shutdown = True

    def _task_done(self, future, chunk):
        exc = future.exception()

        if exc:
            support.logger.error(f'Oops! Handler is failed: {repr(e)}', exc_info=exc)
            support.statsd.increment('failed.messages')
            return

        if hasattr(self._queue, 'cleanup'):
            self._queue.cleanup(chunk)
        support.statsd.increment('successful.messages')
