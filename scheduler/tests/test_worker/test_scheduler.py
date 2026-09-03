from datetime import timedelta

import time_machine
from django.utils import timezone

from scheduler.helpers.utils import current_timestamp
from scheduler.models import TaskType
from scheduler.redis_models import JobModel
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.tests.testtools import SchedulerBaseCase, task_factory
from scheduler.worker import WorkerScheduler, create_worker


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

    def test_scheduler_removes_scheduled_registry_entry_without_job(self):
        # arrange
        task = task_factory(TaskType.CRON)
        job_name = task.job_name
        self.assertIsNotNone(job_name)
        connection = task.rqueue.connection
        registry = task.rqueue.scheduled_job_registry
        connection.delete(JobModel.key_for(job_name))
        registry.add(connection, job_name, current_timestamp() - 10)

        scheduler = WorkerScheduler([task.rqueue], worker_name="fake-worker")
        scheduler._acquire_locks()

        # act
        scheduler.enqueue_scheduled_jobs()

        # assert
        self.assertFalse(registry.exists(connection, job_name))
        self.assertFalse(task.rqueue.queued_job_registry.exists(connection, job_name))

        # act: the next pass schedules the task again
        scheduler.enqueue_scheduled_jobs()

        # assert
        task.refresh_from_db()
        self.assertIsNotNone(task.job_name)
        self.assertNotEqual(job_name, task.job_name)
        self.assertTrue(registry.exists(connection, task.job_name))
