import logging
import os

try:
    from datadog import statsd
except ImportError:


    class FakeStatsd:

        def __getattr__(self, name):
            return lambda *args, **kwgs: None


    statsd = FakeStatsd()


__all__ = (
    'support',
)


logger = logging.getLogger('queue_consumer')
logger.setLevel(level=os.getenv('QUEUE_CONSUMER_LOG_LEVEL', 'DEBUG'))


class Support:

    def __init__(self):
        self.logger = logger
        self.statsd = statsd


support = Support()
