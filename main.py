import argparse
import celery
import celery.states
import collections
import logging
import prometheus_client
import threading
import time


DEFAULT_BROKER = 'redis://redis:6379/0'
DEFAULT_ADDR = '0.0.0.0:8888'

LOG_FORMAT = '[%(asctime)s] %(name)s:%(levelname)s: %(message)s'

TASKS = prometheus_client.Gauge(
    'celery_tasks', 'Number of tasks per state', ['state'])
WORKERS = prometheus_client.Gauge(
    'celery_workers', 'Number of alive workers')


class MonitorThread(threading.Thread):
    """
    MonitorThread is the thread that will collect the data that is later
    exposed from Celery using its eventing system.
    """

    def __init__(self, *args, **kwargs):
        if 'app' in kwargs:
            app = kwargs['app']
            del kwargs['app']
        else:
            app = None
        self.app = app
        self.log = logging.getLogger('monitor')
        super().__init__(*args, **kwargs)

    def run(self):
        self._state = self.app.events.State()
        self._known_states = set()
        self._monitor()

    def _process_event(self, evt):
        self._state.event(evt)
        cnt = collections.Counter(
            t.state for _, t in self._state.tasks.items())
        self._known_states.update(cnt.elements())
        seen_states = set()
        for state_name, count in cnt.items():
            TASKS.labels(state_name).set(count)
            seen_states.add(state_name)
        for state_name in self._known_states - seen_states:
            TASKS.labels(state_name).set(0)
        WORKERS.set(len([w for w in self._state.workers.values() if w.alive]))

    def _monitor(self):
        while True:
            try:
                with self.app.connection() as conn:
                    recv = self.app.events.Receiver(conn, handlers={
                        '*': self._process_event,
                    })
                    recv.capture(limit=None, timeout=None, wakeup=True)
                    self.log.info("Connected to broker")
            except Exception as e:
                self.log.error("Queue connection failed")
                setup_metrics()
                time.sleep(5)


def setup_metrics():
    """
    This initializes the available metrics with default values so that
    even before the first event is received, data can be exposed.
    """
    for state in celery.states.ALL_STATES:
        TASKS.labels(state).set(0)
    WORKERS.set(0)


def start_httpd(addr):
    """
    Starts the exposing HTTPD using the addr provided in a seperate
    thread.
    """
    host, port = addr.split(':')
    logging.info('Starting HTTPD on {}:{}'.format(host, port))
    prometheus_client.start_http_server(int(port), host)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--broker', dest='broker', default=DEFAULT_BROKER,
        help="URL to the Celery broker. Defaults to {}".format(DEFAULT_BROKER))
    parser.add_argument(
        '--addr', dest='addr', default=DEFAULT_ADDR,
        help="Address the HTTPD should listen on. Defaults to {}".format(
            DEFAULT_ADDR))
    parser.add_argument(
        '--verbose', action='store_true', default=False,
        help="Enable verbose logging")
    opts = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    else:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    setup_metrics()
    t = MonitorThread(app=celery.Celery(broker=opts.broker))
    t.start()
    start_httpd(opts.addr)
    t.join()


if __name__ == '__main__':
    main()