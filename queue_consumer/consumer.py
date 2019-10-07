import time
from multiprocessing import cpu_count
from threading import Thread, Event

from .support import support
from .worker import Worker


__all__ = (
    'Consumer',
)


HANDLER_NAME = 'message_handler'


def _process_handler(messages):
    # It calls injected handler and passes messages interator there.
    try:
        globals()[HANDLER_NAME](messages)
    except Exception as exc:
        exc.args += (messages,)
        raise
    # It needs to say parent process about the iterator state and what messages
    # weren't processed. That why it returns iterator or attaches it to exception.
    return messages


class Consumer:

    def __init__(self,
                 queue,
                 handler=None,
                 max_workers=cpu_count(),
                 max_handlers=cpu_count(),
                 messages_bulk_size=1,
                 worker_polling_time=0,
                 pool_initializer=None,
                 with_thread_executor=True):
        handler = getattr(queue, 'handler', handler)
        assert handler, 'Messages handler is not defined!'


        if with_thread_executor:
            from bounded_pool import BoundedThreadPool as Pool
            initializer = pool_initializer

            def _thread_handler(messages):
                try:
                    handler(messages)
                except Exception as exc:
                    exc.args += (messages,)
                    raise
                return messages

            self._handler = _thread_handler
        else:
            from bounded_pool import BoundedProcessPool as Pool

            def initializer():
                import os
                support.logger.debug(f'Pool worker launched in pid {os.getpid()}')
                # It injects handler into child process one time only.
                # Then it will be just called as many time as need.
                globals()[HANDLER_NAME] = handler
                support.logger.debug(f'Handler {HANDLER_NAME!r} is injected')
                pool_initializer and pool_initializer()

            self._handler = _process_handler

        self._queue = queue
        self._executor = Pool(max_handlers, initializer=initializer)
        self._messages_bulk_size = messages_bulk_size
        self._worker_polling_time = worker_polling_time
        self._workers = [self._get_worker() for _ in range(max_workers)]
        self._shutdown = False
        self._no_supervise = Event()

    def start(self):
        for worker in self._workers:
            worker.start()
        support.logger.info('Queue Consumer is started')

    def shutdown(self):
        self._shutdown = True
        self._no_supervise.wait()
        for worker in self._workers:
            worker.shutdown()
        support.logger.info('Queue Consumer is shutdown')

    def supervise(self, blocking=False, polling_time=1):
        support.logger.debug(
            f'Queue Consumer supervising in {"blocking" if blocking else "non-blocking"} mode ...')

        def _supervise():
            while True:

                workers = []
                for worker in self._workers:

                    if worker.is_alive():
                        workers.append(worker)
                    else:
                        support.statsd.increment('revived.workers')

                        new_worker = self._get_worker()
                        new_worker.start()
                        workers.append(new_worker)
                self._workers = workers

                if self._shutdown:
                    self._no_supervise.set()
                    return

                time.sleep(polling_time)

        if blocking:
            _supervise()
        else:
            Thread(target=_supervise, daemon=True).start()

    def _get_worker(self):
        return Worker(self._queue,
                      self._executor,
                      self._handler,
                      self._messages_bulk_size,
                      self._worker_polling_time)
