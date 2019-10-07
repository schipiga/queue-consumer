import time
from functools import partial, wraps
from threading import Thread

from .support import support
from .utils import chunkify


__all__ = (
    'Worker',
)


def capture_error(func):

    @wraps(func)
    def wrapper(*args, **kwgs):
        try:
            return func(*args, **kwgs)
        except Exception as exc:
            support.logger.error(f'Oops! Worker is failed: {repr(exc)}', exc_info=exc)
            raise

    return wrapper


class Worker(Thread):

    def __init__(self, queue, executor, handler, bulk_size=1, polling_time=0):
        self._queue = queue
        self._executor = executor
        self._handler = handler
        self._bulk_size = bulk_size
        self._polling_time = polling_time
        self._shutdown = False
        super().__init__()

    @capture_error
    def run(self):
        support.logger.info('Worker is running')
        while True:
            messages = self._queue.get()
            support.statsd.increment('received.messages', len(messages))

            for chunk in chunkify(messages, self._bulk_size):
                iterator = iter(chunk)
                future = self._executor.submit(self._handler, iterator)
                future.add_done_callback(partial(self._task_done,
                                                    sent_iterator=iterator,
                                                    sent_messages=chunk))

                support.statsd.increment('started.messages', len(chunk))

            if self._shutdown:
                return

            time.sleep(self._polling_time)


    def shutdown(self):
        self._shutdown = True

    @capture_error
    def _task_done(self, future, sent_iterator, sent_messages):
        exc = future.exception()

        failed_messages = list(sent_iterator)
        if exc:
            failed_messages = sent_messages[-len(failed_messages) - 1:]
        if failed_messages:
            successful_messages = sent_messages[:-len(failed_messages)]
        else:
            successful_messages = sent_messages[:]

        if exc:
            support.logger.error(f'Oops! Handler is failed: {repr(exc)}', exc_info=exc)
            support.statsd.increment('failed.messages', len(failed_messages))

        if successful_messages:
            if hasattr(self._queue, 'cleanup'):
                self._queue.cleanup(successful_messages)
            support.statsd.increment('successful.messages', len(successful_messages))
