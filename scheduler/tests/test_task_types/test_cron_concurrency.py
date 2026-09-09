"""Exercise competing transactions on databases with real row-level locks."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from unittest import mock

from django.contrib import admin
from django.db import close_old_connections, connection
from django.test import RequestFactory, TransactionTestCase, skipUnlessDBFeature

from scheduler.admin.task_admin import TaskAdmin
from scheduler.helpers.queues import get_queue
from scheduler.models import Task, TaskType
from scheduler.models.task import success_callback
from scheduler.redis_models import JobModel
from scheduler.tests import conf  # noqa: F401
from scheduler.tests.testtools import task_factory


@skipUnlessDBFeature("has_select_for_update")
class TestCronConcurrency(TransactionTestCase):
    def setUp(self):
        self.queue = get_queue("default")
        self.queue.connection.flushall()
        self.task = task_factory(TaskType.CRON)

    def race(self, actions):
        barrier = Barrier(len(actions))

        def run(action):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                action()
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=len(actions)) as pool:
            futures = [pool.submit(run, action) for action in actions]
            for future in futures:
                future.result(timeout=20)

    def test_concurrent_manual_completions_keep_every_count_and_the_owner(self):
        owner = self.task.job_name
        for _ in range(8):
            self.task.enqueue_to_run()
        jobs = [
            JobModel.get(name, connection=self.queue.connection)
            for name in self.queue.queued_job_registry.all(self.queue.connection)
        ]
        self.race([lambda job=job: success_callback(job, self.queue.connection, None) for job in jobs])
        self.task.refresh_from_db()
        self.assertEqual(self.task.successful_runs, 8)
        self.assertEqual(self.task.job_name, owner)
        self.assertEqual(self.queue.scheduled_job_registry.all(self.queue.connection), [owner])

    def test_stale_scheduler_saves_racing_completion_keep_one_successor(self):
        job = JobModel.get(self.task.job_name, connection=self.queue.connection)
        self.queue.scheduled_job_registry.delete(self.queue.connection, job.name)
        job.prepare_for_execution("racing-worker", self.queue.active_job_registry, self.queue.connection)
        stale_tasks = [Task.objects.get(pk=self.task.pk) for _ in range(4)]
        self.race(
            [lambda: success_callback(job, self.queue.connection, None)]
            + [lambda task=task: task.save(clean=False) for task in stale_tasks]
        )
        self.task.refresh_from_db()
        self.assertEqual(self.task.successful_runs, 1)
        self.assertNotEqual(self.task.job_name, job.name)
        self.assertEqual(self.queue.scheduled_job_registry.all(self.queue.connection), [self.task.job_name])

    def test_concurrent_repair_adopts_only_one_replacement(self):
        self.queue.delete_job(self.task.job_name)
        stale_tasks = [Task.objects.get(pk=self.task.pk) for _ in range(4)]
        self.race([lambda task=task: task.save(clean=False) for task in stale_tasks])
        self.task.refresh_from_db()
        self.assertEqual(self.queue.scheduled_job_registry.all(self.queue.connection), [self.task.job_name])

    def test_scheduler_cannot_reschedule_during_admin_bulk_delete(self):
        from scheduler.worker.scheduler import _reschedule_tasks

        unscheduled = Event()
        rescheduled = Event()
        original_unschedule = Task.unschedule

        def pause_after_unschedule(task, *args, **kwargs):
            result = original_unschedule(task, *args, **kwargs)
            unscheduled.set()
            if not connection.in_atomic_block:
                self.assertTrue(rescheduled.wait(timeout=10), "scheduler did not finish its competing sweep")
            return result

        def sweep_after_unschedule():
            self.assertTrue(unscheduled.wait(timeout=10), "admin did not unschedule the task")
            try:
                _reschedule_tasks()
            finally:
                rescheduled.set()

        task_admin = TaskAdmin(Task, admin.site)
        request = RequestFactory().post("/admin/scheduler/task/")
        queryset = Task.objects.filter(pk=self.task.pk)
        with mock.patch.object(Task, "unschedule", pause_after_unschedule):
            self.race([lambda: task_admin.delete_queryset(request, queryset), sweep_after_unschedule])

        self.assertFalse(Task.objects.filter(pk=self.task.pk).exists())
        self.assertEqual(self.queue.scheduled_job_registry.all(self.queue.connection), [])
