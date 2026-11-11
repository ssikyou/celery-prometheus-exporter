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
