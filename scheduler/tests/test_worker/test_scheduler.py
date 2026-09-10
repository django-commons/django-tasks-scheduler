import os
from datetime import timedelta
from unittest.mock import patch

import time_machine
from django.utils import timezone

from scheduler.helpers.queues import Queue, get_queue
from scheduler.helpers.utils import current_timestamp
from scheduler.models import Task, TaskType
from scheduler.redis_models import JobModel, SchedulerLock
from scheduler.settings import SCHEDULER_CONFIG, logger
from scheduler.tests.testtools import SchedulerBaseCase, task_factory
from scheduler.worker import WorkerScheduler, create_worker
from scheduler.worker.scheduler import _reschedule_tasks


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

    def test_scheduler_only_reschedules_tasks_for_its_queues(self):
        # arrange: task1 on "default", task2 on "low"
        task_default = task_factory(TaskType.CRON, queue="default")
        task_low = task_factory(TaskType.CRON, queue="low")

        job_default = task_default.job_name
        job_low = task_low.job_name
        conn = task_default.rqueue.connection
        conn.delete(JobModel.key_for(job_default))
        conn.delete(JobModel.key_for(job_low))
        task_default.rqueue.scheduled_job_registry.delete(conn, job_default)
        task_low.rqueue.scheduled_job_registry.delete(conn, job_low)

        # Create scheduler only for "default"
        scheduler = WorkerScheduler([task_default.rqueue], worker_name="default-worker")
        scheduler._acquire_locks()

        # act
        scheduler.enqueue_scheduled_jobs()

        # assert: task_default was rescheduled, but task_low was not touched by this scheduler
        task_default.refresh_from_db()
        task_low.refresh_from_db()
        self.assertNotEqual(task_default.job_name, job_default)
        self.assertEqual(task_low.job_name, job_low)

    def test_enqueue_scheduled_jobs__no_locks_held__schedules_nothing(self):
        task = task_factory(TaskType.CRON)
        task.rqueue.delete_job(task.job_name)
        scheduler = WorkerScheduler([task.rqueue], worker_name="fake-worker")

        scheduler.enqueue_scheduled_jobs()

        self.assertFalse(Task.objects.get(id=task.id).is_scheduled())


class TestRescheduleTasks(SchedulerBaseCase):
    def test_all_tasks_scheduled__single_query(self):
        for _ in range(3):
            task_factory(TaskType.CRON)

        with self.assertNumQueries(1):
            _reschedule_tasks(["default"])

    def test_task_disabled_after_the_read__not_scheduled(self):
        task = task_factory(TaskType.CRON)
        task.rqueue.delete_job(task.job_name)

        def disable_task_then_check(queue: Queue, job_names):
            Task.objects.filter(id=task.id).update(enabled=False)
            return set()

        with patch.object(Queue, "pending_job_names", autospec=True, side_effect=disable_task_then_check):
            _reschedule_tasks(["default"])

        self.assertEqual([], task.rqueue.scheduled_job_registry.all(task.rqueue.connection))

    def test_task_deleted_after_the_read__not_scheduled(self):
        task = task_factory(TaskType.CRON)
        task.rqueue.delete_job(task.job_name)

        def delete_task_then_check(queue: Queue, job_names):
            Task.objects.filter(id=task.id).delete()
            return set()

        with patch.object(Queue, "pending_job_names", autospec=True, side_effect=delete_task_then_check):
            _reschedule_tasks(["default"])

        self.assertEqual([], task.rqueue.scheduled_job_registry.all(task.rqueue.connection))

    def test_reschedule_if_needed__task_deleted__schedules_nothing(self):
        task = task_factory(TaskType.CRON)
        stale = Task.objects.get(id=task.id)
        task.rqueue.delete_job(task.job_name)
        Task.objects.filter(id=task.id).delete()

        self.assertFalse(stale.reschedule_if_needed())
        self.assertEqual([], task.rqueue.scheduled_job_registry.all(task.rqueue.connection))

    def test_broken_task__other_tasks_still_scheduled(self):
        broken = task_factory(TaskType.CRON)
        healthy = task_factory(TaskType.CRON)
        for task in (broken, healthy):
            task.rqueue.delete_job(task.job_name)
        Task.objects.filter(id=broken.id).update(cron_string="not a cron string")

        with self.assertLogs(logger, "ERROR"):
            _reschedule_tasks(["default"])

        self.assertTrue(Task.objects.get(id=healthy.id).is_scheduled())


class TestSchedulerLocks(SchedulerBaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.queue = get_queue("default")
        self.lock = SchedulerLock(self.queue.name)

    def _scheduler(self) -> WorkerScheduler:
        scheduler = WorkerScheduler([self.queue], worker_name="fake-worker")
        scheduler._acquire_locks()
        return scheduler

    def test_heartbeat__lock_held__extends_it(self):
        scheduler = self._scheduler()
        self.queue.connection.expire(self.lock._locking_key, 5)

        scheduler.heartbeat()

        self.assertIn(self.queue.name, scheduler._locks)
        self.assertGreater(self.queue.connection.ttl(self.lock._locking_key), 5)

    def test_heartbeat__lock_taken_over__stops_scheduling_the_queue(self):
        scheduler = self._scheduler()
        # The lock expired, and another scheduler took it.
        self.queue.connection.set(self.lock._locking_key, "other-scheduler")

        scheduler.heartbeat()

        self.assertEqual({}, scheduler._locks)
        self.assertEqual([], scheduler._scheduled_job_registries)
        self.assertEqual(b"other-scheduler", self.lock.value(self.queue.connection))

    def test_heartbeat__lock_taken_over_by_a_scheduler_with_the_same_pid__stops_scheduling_the_queue(self):
        # Containers commonly share pids, so the pid alone must not identify the lock holder.
        first = self._scheduler()
        self.queue.connection.delete(self.lock._locking_key)
        second = self._scheduler()

        first.heartbeat()

        self.assertEqual({}, first._locks)
        self.assertIn(self.queue.name, second._locks)

    def test_release_locks__lock_taken_over__leaves_it(self):
        scheduler = self._scheduler()
        self.queue.connection.set(self.lock._locking_key, "other-scheduler")

        scheduler.release_locks()

        self.assertEqual(b"other-scheduler", self.lock.value(self.queue.connection))

    def test_release_locks__lock_held__releases_it(self):
        scheduler = self._scheduler()

        scheduler.release_locks()

        self.assertIsNone(self.lock.value(self.queue.connection))

    def test_scheduler_pid__reports_the_lock_holders_pid(self):
        self._scheduler()

        self.assertEqual(os.getpid(), self.queue.scheduler_pid)
