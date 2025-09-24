from datetime import timedelta

import time_machine
from django.utils import timezone

from scheduler.models import TaskType
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.tests.testtools import SchedulerBaseCase, task_factory
from scheduler.worker import WorkerScheduler
from scheduler.worker import create_worker


class TestWorkerScheduler(SchedulerBaseCase):
    def test_create_worker_with_scheduler__scheduler_started(self):
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 1
        worker = create_worker("default", name="test", burst=True, with_scheduler=True)
        worker.bootstrap()
        self.assertIsNotNone(worker.scheduler)
        worker.stop_scheduler()
        self.assertIsNone(worker.scheduler)

    def test_scheduler_schedules_tasks(self):
        with time_machine.travel(0.0, tick=False) as traveller:
            # arrange
            task = task_factory(TaskType.ONCE, scheduled_time=timezone.now() + timedelta(seconds=50))
            self.assertIsNotNone(task.job_name)
            self.assertFalse(task.rqueue.queued_job_registry.exists(task.rqueue.connection, task.job_name))
            self.assertTrue(task.rqueue.scheduled_job_registry.exists(task.rqueue.connection, task.job_name))

            scheduler = WorkerScheduler([task.rqueue], worker_name="fake-worker")

            # act
            traveller.move_to(50)
            scheduler._acquire_locks()
            scheduler.enqueue_scheduled_jobs()

            # assert
            self.assertIsNotNone(task.job_name)
            self.assertTrue(task.rqueue.queued_job_registry.exists(task.rqueue.connection, task.job_name))
            self.assertFalse(task.rqueue.scheduled_job_registry.exists(task.rqueue.connection, task.job_name))
