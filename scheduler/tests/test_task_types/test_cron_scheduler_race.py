"""Reproduce automatic scheduler/completion races with PostgreSQL and a broker."""

import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from unittest import skipUnless
from unittest.mock import patch

from django.db import close_old_connections, connection
from django.test import TransactionTestCase

from scheduler.helpers.queues import Queue, get_queue
from scheduler.models import TaskType
from scheduler.redis_models import JobModel, JobStatus
from scheduler.tests import conf  # noqa: F401
from scheduler.tests.testtools import task_factory
from scheduler.worker.scheduler import _reschedule_tasks


@skipUnless(connection.vendor == "postgresql", "Requires PostgreSQL transaction isolation and row locks")
class TestCronSchedulerRace(TransactionTestCase):
    def setUp(self):
        self.queue = get_queue("default")
        if port := os.getenv("BROKER_PORT"):
            self.assertEqual(
                self.queue.connection.connection_pool.connection_kwargs["port"],
                int(port),
                "Test configuration ignored BROKER_PORT; refusing to flush Redis",
            )
        self.queue.connection.flushall()
        self.task = task_factory(TaskType.CRON)
        job = JobModel.get(self.task.job_name, connection=self.queue.connection)
        self.queue.enqueue_job(job)
        self.job, _ = Queue.dequeue_any([self.queue], timeout=None, connection=self.queue.connection)
        self.assertIsNotNone(self.job)

    @contextmanager
    def paused_sweep(self):
        """Pause after the sweep's first real Task SELECT, keeping its snapshot."""
        snapshot_read = Event()
        resume = Event()

        def pause_after_read(execute, sql, params, many, context):
            result = execute(sql, params, many, context)
            if not snapshot_read.is_set() and sql.startswith("SELECT") and self.task._meta.db_table in sql:
                snapshot_read.set()
                if not resume.wait(timeout=10):
                    raise TimeoutError("Completion did not release the scheduler sweep")
            return result

        def sweep():
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '10s'")
                with connection.execute_wrapper(pause_after_read):
                    _reschedule_tasks()
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = None

            def start_sweep():
                nonlocal future
                self.assertIsNone(future, "Only one automatic sweep is needed")
                future = pool.submit(sweep)
                self.assertTrue(snapshot_read.wait(timeout=10), "Scheduler did not read its task snapshot")

            try:
                yield start_sweep
            finally:
                resume.set()
                if future is not None:
                    future.result(timeout=20)

    def assert_one_successor(self):
        self.task.refresh_from_db()
        scheduled = self.queue.scheduled_job_registry.all(self.queue.connection)
        self.assertEqual(self.job.status, JobStatus.FINISHED)
        self.assertEqual(
            {"scheduled_jobs": len(scheduled), "successful_runs": self.task.successful_runs},
            {"scheduled_jobs": 1, "successful_runs": 1},
        )
        self.assertEqual(scheduled, [self.task.job_name])
        self.assertNotEqual(self.task.job_name, self.job.name)
        self.assertIsNotNone(self.task.last_successful_run)
        self.assertEqual(self.task.failed_runs, 0)
        self.assertEqual(self.queue.queued_job_registry.all(self.queue.connection), [])
        self.assertEqual(self.queue.active_job_registry.all(self.queue.connection), [])

    def test_sweep_read_before_completion_preserves_successor_and_counter(self):
        with self.paused_sweep() as start_sweep:
            start_sweep()
            self.queue.run_sync(self.job)
        self.assert_one_successor()

    def test_sweep_read_during_successor_creation_keeps_one_chain(self):
        create_job = Queue.create_and_enqueue_job
        with self.paused_sweep() as start_sweep:

            def create_after_sweep_read(queue, *args, **kwargs):
                # The old callback has published job_name=NULL by this point.
                start_sweep()
                return create_job(queue, *args, **kwargs)

            with patch.object(Queue, "create_and_enqueue_job", create_after_sweep_read):
                self.queue.run_sync(self.job)
        self.assert_one_successor()
