import time
from multiprocessing import cpu_count
from threading import Thread

from bounded_pool import BoundedThreadPool

from .support import support
from .worker import Worker


__all__ = (
    'Consumer',
)


class Consumer:

    def __init__(self,
                 queue,
                 max_workers=cpu_count() * 4,
                 max_handlers=cpu_count() * 4 * 4,
                 messages_bulk_size=1,
                 worker_polling_time=0):
        self._queue = queue
        self._executor = BoundedThreadPool(max_handlers)
        self._messages_bulk_size = messages_bulk_size
        self._worker_polling_time = worker_polling_time
        self._workers = [self._get_worker() for _ in range(max_workers)]
        self._shutdown = False

    def start(self):
        for worker in self._workers:
            worker.start()

    def shutdown(self):
        self._shutdown = True
        for worker in self._workers:
            worker.shutdown()

    def supervise(self, blocking=False, polling_time=1):

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

                if self._shutdown:
                    return

                self._workers = workers
                time.sleep(polling_time)

        if blocking:
            _supervise()
        else:
            Thread(target=_supervise, daemon=True).start()

    def _get_worker(self):
        return Worker(self._queue,
                      self._executor,
                      self._messages_bulk_size,
                      self._worker_polling_time)
