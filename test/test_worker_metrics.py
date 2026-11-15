from unittest import TestCase
from unittest.mock import MagicMock, patch

import celery
from celery.events import Event
from celery.utils import uuid
from prometheus_client import REGISTRY

from celery_prometheus_exporter import (
    ActiveTaskMonitoringThread, MonitorThread, WorkerMonitoringThread)


def get_app():
    return celery.Celery(broker='memory://', backend='cache+memory://')


def sample(name, **labels):
    return REGISTRY.get_sample_value(name, labels=labels)


class TestWorkerTaskEvents(TestCase):
    def test_counts_task_events_per_worker(self):
        m = MonitorThread(app=get_app())
        task_uuid = uuid()
        host = 'w-events@host'
        before = sample('celery_worker_tasks_total', hostname=host,
                        state='SUCCESS') or 0

        m._process_event(Event('task-received', uuid=task_uuid, name='t',
                               hostname=host, clock=0, local_received=1))
        m._process_event(Event('task-started', uuid=task_uuid,
                               hostname=host, clock=1, local_received=2))
        m._process_event(Event('task-succeeded', uuid=task_uuid, runtime=12.5,
                               hostname=host, clock=2, local_received=3))
        m._process_event(Event('task-failed', uuid=uuid(), hostname=host,
                               clock=3, local_received=4))

        assert sample('celery_worker_tasks_total', hostname=host,
                      state='RECEIVED') == 1
        assert sample('celery_worker_tasks_total', hostname=host,
                      state='STARTED') == 1
        assert sample('celery_worker_tasks_total', hostname=host,
                      state='SUCCESS') == before + 1
        assert sample('celery_worker_tasks_total', hostname=host,
                      state='FAILURE') == 1
        assert sample('celery_worker_task_runtime_seconds_count',
                      hostname=host) == 1
        assert sample('celery_worker_task_runtime_seconds_sum',
                      hostname=host) == 12.5
        assert sample('celery_worker_task_runtime_seconds_bucket',
                      hostname=host, le='20.0') == 1
        assert sample('celery_worker_task_runtime_seconds_bucket',
                      hostname=host, le='10.0') == 0

    def test_event_without_hostname_is_ignored(self):
        m = MonitorThread(app=get_app())
        m._process_event(Event('task-succeeded', uuid=uuid(), runtime=1,
                               clock=0, local_received=1))
        assert sample('celery_worker_tasks_total', hostname='',
                      state='SUCCESS') is None


class TestWorkerUp(TestCase):
    def test_up_and_down(self):
        app = get_app()
        w = WorkerMonitoringThread(app=app)
        with patch.object(app.control, 'ping') as ping:
            ping.return_value = [{'w-up1@h': {'ok': 'pong'}},
                                 {'w-up2@h': {'ok': 'pong'}}]
            w.update_workers_count()
            assert sample('celery_workers') == 2
            assert sample('celery_worker_up', hostname='w-up1@h') == 1
            assert sample('celery_worker_up', hostname='w-up2@h') == 1

            ping.return_value = [{'w-up1@h': {'ok': 'pong'}}]
            w.update_workers_count()
            assert sample('celery_workers') == 1
            assert sample('celery_worker_up', hostname='w-up1@h') == 1
            assert sample('celery_worker_up', hostname='w-up2@h') == 0

            ping.return_value = [{'w-up2@h': {'ok': 'pong'}},
                                 {'w-up1@h': {'ok': 'pong'}}]
            w.update_workers_count()
            assert sample('celery_worker_up', hostname='w-up2@h') == 1


class TestWorkerActive(TestCase):
    def _thread(self, active, reserved, stats):
        app = get_app()
        inspect = MagicMock()
        inspect.active.return_value = active
        inspect.reserved.return_value = reserved
        inspect.stats.return_value = stats
        app.control.inspect = MagicMock(return_value=inspect)
        return ActiveTaskMonitoringThread(app=app), inspect

    def test_per_worker_active_reserved_concurrency(self):
        a, inspect = self._thread(
            active={'w-a1@h': [{}, {}, {}], 'w-a2@h': []},
            reserved={'w-a1@h': [{}], 'w-a2@h': []},
            stats={'w-a1@h': {'pool': {'max-concurrency': 30}},
                   'w-a2@h': {'pool': {'max-concurrency': 4}}})
        a.update_active_count()

        assert sample('celery_tasks_active') == 3
        assert sample('celery_worker_tasks_active', hostname='w-a1@h') == 3
        assert sample('celery_worker_tasks_active', hostname='w-a2@h') == 0
        assert sample('celery_worker_tasks_reserved', hostname='w-a1@h') == 1
        assert sample('celery_worker_concurrency', hostname='w-a1@h') == 30
        assert sample('celery_worker_concurrency', hostname='w-a2@h') == 4

        # w-a2 stops replying: its series are dropped
        inspect.active.return_value = {'w-a1@h': [{}]}
        inspect.reserved.return_value = {'w-a1@h': []}
        inspect.stats.return_value = {
            'w-a1@h': {'pool': {'max-concurrency': 30}}}
        a.update_active_count()
        assert sample('celery_tasks_active') == 1
        assert sample('celery_worker_tasks_active', hostname='w-a1@h') == 1
        assert sample('celery_worker_tasks_active', hostname='w-a2@h') is None
        assert sample('celery_worker_concurrency', hostname='w-a2@h') is None

    def test_no_reply_keeps_total_and_drops_workers(self):
        a, inspect = self._thread(
            active={'w-n1@h': [{}, {}]}, reserved={}, stats={})
        a.update_active_count()
        assert sample('celery_tasks_active') == 2

        inspect.active.return_value = None
        inspect.reserved.return_value = None
        inspect.stats.return_value = None
        a.update_active_count()
        # total keeps the last value like before, per worker series dropped
        assert sample('celery_tasks_active') == 2
        assert sample('celery_worker_tasks_active', hostname='w-n1@h') is None


class TestTaskErrors(TestCase):
    def test_normalize_exception(self):
        from celery_prometheus_exporter import normalize_exception as n
        old = ("DownloadError('HTTPError: 403 Client Error: Forbidden for url: "
               "https://v16m.tiktokcdn-us.com/abc/6af84afc/video/tos/a=1233&x=1')")
        assert n(old) == 'DownloadError: HTTP 403 from tiktokcdn-us.com'
        assert n("DownloadError('HTTP 404 from api16-normal-c-alisg.tiktokv.com')") \
            == 'DownloadError: HTTP 404 from tiktokv.com'
        assert n("DownloadError('HTTP 403 from fastdl.muscdn.app')") \
            == 'DownloadError: HTTP 403 from muscdn.app'
        assert n("FileNotFoundError(2, 'No such file or directory')") \
            == "FileNotFoundError: 2, 'No such file or directory'"
        assert n("ConnectionError('Error 110 connecting to 45.79.249.219:6379. "
                 "Connection timed out.')") \
            == 'ConnectionError: Error 110 connecting to IP. Connection timed out.'
        assert n("DownloadError('incomplete download: 1048576/2345678 bytes')") \
            == 'DownloadError: incomplete download: N/N bytes'
        assert n("TimeLimitExceeded(1530,)") == 'TimeLimitExceeded: N,'
        assert n(None) == 'unknown'
        assert len(n("ValueError('" + 'x' * 500 + "')")) == 100

    def test_counts_failed_and_retried_by_error(self):
        m = MonitorThread(app=get_app())
        host = 'w-err@host'
        exc = "DownloadError('HTTP 403 from v19.tiktokcdn-us.com')"
        err = 'DownloadError: HTTP 403 from tiktokcdn-us.com'
        m._process_event(Event('task-retried', uuid=uuid(), hostname=host,
                               exception=exc, clock=0, local_received=1))
        m._process_event(Event('task-retried', uuid=uuid(), hostname=host,
                               exception=exc.replace('v19', 'v16m'),
                               clock=1, local_received=2))
        m._process_event(Event('task-failed', uuid=uuid(), hostname=host,
                               exception=exc, clock=2, local_received=3))
        m._process_event(Event('task-succeeded', uuid=uuid(), hostname=host,
                               runtime=1, clock=3, local_received=4))
        assert sample('celery_task_errors_total', hostname=host,
                      state='RETRY', error=err) == 2
        assert sample('celery_task_errors_total', hostname=host,
                      state='FAILURE', error=err) == 1

    def test_error_labels_are_capped(self):
        import celery_prometheus_exporter as cpe
        m = MonitorThread(app=get_app())
        host = 'w-cap@host'
        with patch.object(cpe, 'TASK_ERRORS_MAX_LABELS', 0), \
                patch.object(cpe, '_error_labels', set()):
            m._process_event(Event('task-failed', uuid=uuid(), hostname=host,
                                   exception="KeyError('abc')",
                                   clock=0, local_received=1))
        assert sample('celery_task_errors_total', hostname=host,
                      state='FAILURE', error='KeyError: other') == 1
        assert sample('celery_task_errors_total', hostname=host,
                      state='FAILURE', error="KeyError: abc") is None
