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
                    future.add_done_callback(partial(self._task_done,
                                                     successful_messages=chunk,
                                                     started_messages=chunk[:]))

                    support.statsd.increment('started.messages', len(chunk))

                if self._shutdown:
                    return

                time.sleep(self._polling_time)

        except Exception as exc:
            support.logger.error(f'Oops! Worker is failed: {repr(e)}', exc_info=exc)

    def shutdown(self):
        self._shutdown = True

    def _task_done(self, future, successful_messages, started_messages):
        exc = future.exception()

        if exc and (len(started_messages) == 1):
            failed_messages = started_messages
            successful_messages[:] = []
        else:
            failed_messages = list(set(started_messages) - set(successful_messages))

        if exc:
            support.logger.error(f'Oops! Handler is failed: {repr(e)}', exc_info=exc)
            if failed_messages:
                support.statsd.increment('failed.messages', len(failed_messages))

        if successful_messages:
            if hasattr(self._queue, 'cleanup'):
                self._queue.cleanup(successful_messages)
            support.statsd.increment('successful.messages', len(successful_messages))
