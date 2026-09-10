import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event, current_thread, main_thread
from unittest.mock import patch

from django.db import close_old_connections, connection
from django.db.models.query import QuerySet
from django.test import TransactionTestCase

from scheduler.helpers.queues import Queue, get_queue
from scheduler.models import Task, TaskType
from scheduler.redis_models import JobModel
from scheduler.tests import conf  # noqa: F401
from scheduler.tests.testtools import task_factory
from scheduler.worker.scheduler import _reschedule_tasks


class TestSQLiteRace(TransactionTestCase):
    def setUp(self):
        with connection.cursor() as cursor:
            if os.environ.get("SQLITE_KIND") == "wal":
                cursor.execute("PRAGMA journal_mode=WAL")
        self.queue = get_queue("default")
        self.queue.connection.flushall()
        self.task = task_factory(TaskType.CRON)
        job = JobModel.get(self.task.job_name, connection=self.queue.connection)
        self.queue.enqueue_job(job)
        self.job, _ = Queue.dequeue_any([self.queue], timeout=None, connection=self.queue.connection)

    def test_control_completion_then_rescheduling(self):
        self.queue.run_sync(self.job)
        _reschedule_tasks()
        self.task.refresh_from_db()
        scheduled = self.queue.scheduled_job_registry.all(self.queue.connection)
        self.assertEqual(self.job.status.value, "finished")
        self.assertEqual(self.task.successful_runs, 1)
        self.assertEqual(self.task.failed_runs, 0)
        self.assertEqual(scheduled, [self.task.job_name])

    def test_rescheduling_reads_task_then_completion_finishes(self):
        snapshot_loaded = Event()
        resume_rescheduling = Event()
        original_first = QuerySet.first

        def pause_after_first(queryset):
            result = original_first(queryset)
            if current_thread() is not main_thread() and queryset.model is Task and not snapshot_loaded.is_set():
                snapshot_loaded.set()
                if not resume_rescheduling.wait(timeout=15):
                    raise TimeoutError("Completion did not release rescheduling")
            return result

        def reschedule_tasks():
            close_old_connections()
            try:
                _reschedule_tasks()
            finally:
                close_old_connections()

        with patch.object(QuerySet, "first", pause_after_first), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(reschedule_tasks)
            try:
                self.assertTrue(snapshot_loaded.wait(timeout=10))
                self.queue.run_sync(self.job)
            finally:
                resume_rescheduling.set()
            future.result(timeout=20)
        self.task.refresh_from_db()
        scheduled = self.queue.scheduled_job_registry.all(self.queue.connection)
        self.assertEqual(self.job.status.value, "finished")
        self.assertEqual(self.task.successful_runs, 1)
        self.assertEqual(self.task.failed_runs, 0)
        self.assertEqual(scheduled, [self.task.job_name])
