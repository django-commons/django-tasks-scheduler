"""Completion bookkeeping survives notification and successor-scheduling failures."""

from datetime import timedelta
from smtplib import SMTPException
from unittest.mock import patch

from django.db import connection
from django.test import TransactionTestCase
from django.utils import timezone
from redis.exceptions import ConnectionError as RedisConnectionError

from scheduler.helpers.queues import Queue, get_queue
from scheduler.models import Task, TaskType
from scheduler.models.task import failure_callback
from scheduler.redis_models import JobModel, JobStatus
from scheduler.tests import conf  # noqa: F401
from scheduler.tests.testtools import SchedulerBaseCase, task_factory


class TestCompletionMail(TransactionTestCase):
    def setUp(self):
        self.queue = get_queue()
        self.queue.connection.flushall()

    def test_failure_mail_runs_after_the_completion_transaction(self):
        for task_type in TaskType:
            with self.subTest(task_type=task_type):
                task = task_factory(task_type)
                job = JobModel.get(task.job_name, connection=self.queue.connection)
                observed = []

                def send_mail(*args, task=task, observed=observed):
                    observed.append((connection.in_atomic_block, Task.objects.get(pk=task.pk).failed_runs))

                with patch("scheduler.models.task.mail_admins", side_effect=send_mail):
                    failure_callback(job, self.queue.connection, None)

                self.assertEqual(observed, [(False, 1)])

    def test_mail_failure_does_not_interrupt_completion(self):
        for task_type in TaskType:
            with self.subTest(task_type=task_type):
                task = task_factory(task_type)
                job = JobModel.get(task.job_name, connection=self.queue.connection)

                with patch("scheduler.models.task.mail_admins", side_effect=SMTPException("mail offline")):
                    failure_callback(job, self.queue.connection, None)

                task.refresh_from_db()
                self.assertEqual(task.failed_runs, 1)
                self.assertIsNotNone(task.last_failed_run)


class TestCompletionBrokerErrors(SchedulerBaseCase):
    def test_successor_enqueue_failure_preserves_the_completed_run(self):
        for task_type in (TaskType.REPEATABLE, TaskType.CRON):
            for failed in (False, True):
                with self.subTest(task_type=task_type, failed=failed):
                    callable_name = "scheduler.tests.jobs.failing_job" if failed else "scheduler.tests.jobs.test_job"
                    task = task_factory(task_type, callable_name=callable_name)
                    queue = task.rqueue
                    job = JobModel.get(task.job_name, connection=queue.connection)
                    queue.scheduled_job_registry.delete(queue.connection, job.name)

                    with patch.object(Queue, "create_and_enqueue_job", side_effect=RedisConnectionError("offline")):
                        queue.run_sync(job)

                    task.refresh_from_db()
                    self.assertEqual(task.successful_runs, 0 if failed else 1)
                    self.assertEqual(task.failed_runs, 1 if failed else 0)
                    self.assertIsNotNone(task.last_failed_run if failed else task.last_successful_run)
                    self.assertEqual(job.status, JobStatus.FAILED if failed else JobStatus.FINISHED)
                    task.save(clean=False)
                    self.assertNotEqual(task.job_name, job.name)
                    names = queue.scheduled_job_registry.all(queue.connection)
                    self.assertEqual(
                        [name for name in names if name.startswith(f"{task.queue}:{task.pk}:")], [task.job_name]
                    )

    def test_unreadable_schedule_is_reported_until_a_successful_save(self):
        for task_type in (TaskType.ONCE, TaskType.REPEATABLE):
            with self.subTest(task_type=task_type):
                task = task_factory(task_type)
                owner = task.job_name
                with patch.object(
                    type(task.rqueue.connection), "pipeline", side_effect=RedisConnectionError("offline")
                ):
                    task.save(clean=False)

                self.assertFalse(task.schedule_updated)
                self.assertEqual(Task.objects.get(pk=task.pk).job_name, owner)
                task.save(clean=False)
                self.assertTrue(task.schedule_updated)
                self.assertEqual(task.job_name, owner)

    def test_normal_scheduling_noops_do_not_report_a_broker_error(self):
        for task_type in (TaskType.ONCE, TaskType.REPEATABLE):
            with self.subTest(task_type=task_type):
                task = task_factory(task_type, enabled=False)
                self.assertTrue(task.schedule_updated)
                task.enabled = True
                task.scheduled_time = timezone.now() - timedelta(hours=1)
                task.repeat = 0
                task.save(clean=False)
                self.assertIsNone(task.job_name)
                self.assertTrue(task.schedule_updated)

    def test_enqueue_failure_reports_a_saved_but_unscheduled_task(self):
        for task_type in (TaskType.ONCE, TaskType.REPEATABLE):
            with self.subTest(task_type=task_type):
                task = task_factory(task_type, instance_only=True)
                with patch.object(Queue, "create_and_enqueue_job", side_effect=RedisConnectionError("offline")):
                    task.save()

                self.assertFalse(task.schedule_updated)
                self.assertIsNone(Task.objects.get(pk=task.pk).job_name)
                task.save()
                self.assertTrue(task.schedule_updated)
                self.assertTrue(task.is_scheduled())
