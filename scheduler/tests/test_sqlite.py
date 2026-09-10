import json
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event, current_thread, main_thread
from unittest.mock import patch

import django
from django.db import close_old_connections, connection
from django.db.models.query import QuerySet
from django.test import TransactionTestCase

import scheduler
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
            cursor.execute("PRAGMA journal_mode")
            self.journal_mode = cursor.fetchone()[0]
        self.queue = get_queue("default")
        self.assertEqual(self.queue.connection.connection_pool.connection_kwargs["port"], int(os.getenv("BROKER_PORT", "6379")))
        self.queue.connection.flushall()
        self.task = task_factory(TaskType.CRON)
        job = JobModel.get(self.task.job_name, connection=self.queue.connection)
        self.queue.enqueue_job(job)
        self.job, _ = Queue.dequeue_any([self.queue], timeout=None, connection=self.queue.connection)
        self.errors = []
        self.snapshot_atomic = None

    def report(self):
        self.task.refresh_from_db()
        scheduled = self.queue.scheduled_job_registry.all(self.queue.connection)
        result = {
            "test": self._testMethodName,
            "library": scheduler.__file__,
            "django": django.get_version(),
            "sqlite_kind": os.environ.get("SQLITE_KIND"),
            "journal_mode": self.journal_mode,
            "snapshot_atomic": self.snapshot_atomic,
            "job_status": self.job.status.value,
            "successful_runs": self.task.successful_runs,
            "failed_runs": self.task.failed_runs,
            "scheduled_count": len(scheduled),
            "scheduled_names": scheduled,
            "errors": self.errors,
        }
        print("RESULT " + json.dumps(result), flush=True)
        self.assertEqual(self.errors, [])
        self.assertEqual(self.job.status.value, "finished")
        self.assertEqual(self.task.successful_runs, 1)
        self.assertEqual(self.task.failed_runs, 0)
        self.assertEqual(len(scheduled), 1)

    def test_control_completion_then_sweep(self):
        self.queue.run_sync(self.job)
        _reschedule_tasks()
        self.report()

    def test_sweep_reads_task_then_completion_finishes(self):
        snapshot_loaded = Event()
        resume_sweep = Event()
        original_first = QuerySet.first

        def pause_after_first(queryset):
            result = original_first(queryset)
            if current_thread() is not main_thread() and queryset.model is Task and not snapshot_loaded.is_set():
                self.snapshot_atomic = connection.in_atomic_block
                snapshot_loaded.set()
                if not resume_sweep.wait(timeout=15):
                    raise TimeoutError("Completion did not release sweep")
            return result

        def sweep():
            close_old_connections()
            try:
                _reschedule_tasks()
            finally:
                close_old_connections()

        with patch.object(QuerySet, "first", pause_after_first), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(sweep)
            try:
                self.assertTrue(snapshot_loaded.wait(timeout=10))
                self.queue.run_sync(self.job)
            finally:
                resume_sweep.set()
            try:
                future.result(timeout=20)
            except Exception as exc:
                cause = exc.__cause__
                self.errors.append({
                    "exception": type(exc).__name__,
                    "message": str(exc),
                    "sqlite_errorcode": getattr(cause, "sqlite_errorcode", None),
                    "sqlite_errorname": getattr(cause, "sqlite_errorname", None),
                })
        self.report()
