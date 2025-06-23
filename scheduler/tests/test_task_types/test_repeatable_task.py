from datetime import timedelta

from django.core.exceptions import ValidationError
from django.test import override_settings
from django.utils import timezone

from scheduler import settings
from scheduler.models import TaskType, Task
from scheduler.redis_models import JobModel
from scheduler.tests.test_task_types.test_task_model import BaseTestCases
from scheduler.tests.testtools import task_factory, _get_task_scheduled_job_from_registry
from scheduler.types import SchedulerConfiguration


class TestRepeatableTask(BaseTestCases.TestSchedulableTask):
    queue_name = settings.get_queue_names()[0]

    def test_create_task_error(self):
        scheduled_time = timezone.now()
        task = Task.objects.create(
            name="konichiva_every_2s",
            callable="scheduler.tests.jobs.test_args_kwargs",
            task_type=TaskType.REPEATABLE,
            interval=333,
            interval_unit="seconds",
            queue="default",
            enabled=True,
            scheduled_time=scheduled_time,
        )
        self.assertEqual(task.name, "konichiva_every_2s")
        self.assertEqual(task.callable, "scheduler.tests.jobs.test_args_kwargs")
        self.assertEqual(task.task_type, TaskType.REPEATABLE)
        self.assertEqual(task.interval, 333)
        self.assertEqual(task.interval_unit, "seconds")

    def test_create_task_without_scheduled_time(self):
        settings.SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 10
        task = Task.objects.create(
            name="konichiva_every_2s",
            callable="scheduler.tests.jobs.test_args_kwargs",
            task_type=TaskType.REPEATABLE,
            interval=33,
            interval_unit="seconds",
            queue="default",
            enabled=True,
        )
        self.assertAlmostEqual(task.scheduled_time.timestamp(), timezone.now().timestamp(), delta=2)
        self.assertEqual(task.name, "konichiva_every_2s")
        self.assertEqual(task.callable, "scheduler.tests.jobs.test_args_kwargs")
        self.assertEqual(task.task_type, TaskType.REPEATABLE)
        self.assertEqual(task.interval, 33)
        self.assertEqual(task.interval_unit, "seconds")

    def test_unschedulable_old_job(self):
        task = task_factory(TaskType.REPEATABLE, scheduled_time=timezone.now() - timedelta(hours=1), repeat=0)
        self.assertFalse(task.is_scheduled())

    def test_schedulable_old_job_repeat_none(self):
        # If repeat is None, the job should be scheduled
        task = task_factory(TaskType.REPEATABLE, scheduled_time=timezone.now() - timedelta(hours=1), repeat=None)
        self.assertTrue(task.is_scheduled())

    def test_clean(self):
        task = task_factory(TaskType.REPEATABLE)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        task.interval = 1
        task.success_ttl = -1
        self.assertIsNone(task.clean())

    def test_clean_seconds(self):
        task = task_factory(TaskType.REPEATABLE)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        task.interval = 60
        task.success_ttl = -1
        task.interval_unit = "seconds"
        task.clean()

    @override_settings(SCHEDULER_CONFIG=SchedulerConfiguration(SCHEDULER_INTERVAL=10))
    def test_clean_too_frequent(self):
        task = task_factory(TaskType.REPEATABLE)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        task.interval = 2  # Smaller than 10
        task.success_ttl = -1
        task.interval_unit = "seconds"
        with self.assertRaises(ValidationError):
            task.clean_interval_unit()

    def test_clean_short_result_ttl(self):
        task = task_factory(TaskType.REPEATABLE)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        task.interval = 1
        task.repeat = 1
        task.result_ttl = 3599
        task.interval_unit = "hours"
        task.repeat = 42
        with self.assertRaises(ValidationError):
            task.clean_result_ttl()

    def test_clean_indefinite_result_ttl(self):
        task = task_factory(TaskType.REPEATABLE)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        task.interval = 1
        task.result_ttl = -1
        task.interval_unit = "hours"
        task.clean_result_ttl()

    def test_clean_undefined_result_ttl(self):
        task = task_factory(TaskType.REPEATABLE)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        task.interval = 1
        task.interval_unit = "hours"
        task.clean_result_ttl()

    def test_interval_seconds_weeks(self):
        task = task_factory(TaskType.REPEATABLE, interval=2, interval_unit="weeks")
        self.assertEqual(1209600.0, task.interval_seconds())

    def test_interval_seconds_days(self):
        task = task_factory(TaskType.REPEATABLE, interval=2, interval_unit="days")
        self.assertEqual(172800.0, task.interval_seconds())

    def test_interval_seconds_hours(self):
        task = task_factory(TaskType.REPEATABLE, interval=2, interval_unit="hours")
        self.assertEqual(7200.0, task.interval_seconds())

    def test_interval_seconds_minutes(self):
        task = task_factory(TaskType.REPEATABLE, interval=15, interval_unit="minutes")
        self.assertEqual(900.0, task.interval_seconds())

    def test_interval_seconds_seconds(self):
        task = task_factory(TaskType.REPEATABLE, interval=15, interval_unit="seconds")
        self.assertEqual(15.0, task.interval_seconds())

    def test_result_interval(self):
        task = task_factory(TaskType.REPEATABLE)
        entry = _get_task_scheduled_job_from_registry(task)
        self.assertEqual(entry.meta["interval"], 3600)

    def test_repeat(self):
        task = task_factory(TaskType.REPEATABLE, repeat=10)
        entry = _get_task_scheduled_job_from_registry(task)
        self.assertEqual(entry.meta["repeat"], 10)

    def test_repeat_old_job_exhausted(self):
        base_time = timezone.now()
        task = task_factory(TaskType.REPEATABLE, scheduled_time=base_time - timedelta(hours=10), repeat=10)
        self.assertEqual(task.is_scheduled(), False)

    def test_repeat_old_job_last_iter(self):
        base_time = timezone.now()
        task = task_factory(TaskType.REPEATABLE, scheduled_time=base_time - timedelta(hours=9, minutes=30), repeat=10)
        self.assertEqual(task.repeat, 0)
        self.assertEqual(task.is_scheduled(), True)

    def test_repeat_old_job_remaining(self):
        base_time = timezone.now()
        task = task_factory(TaskType.REPEATABLE, scheduled_time=base_time - timedelta(minutes=30), repeat=5)
        self.assertEqual(task.repeat, 4)
        self.assertEqual(task.scheduled_time, base_time + timedelta(minutes=30))
        self.assertEqual(task.is_scheduled(), True)

    def test_repeat_none_interval_2_min(self):
        base_time = timezone.now()
        task = task_factory(TaskType.REPEATABLE, scheduled_time=base_time - timedelta(minutes=29), repeat=None)
        task.interval = 120
        task.interval_unit = "seconds"
        task.save()
        self.assertTrue(task.scheduled_time > base_time)
        self.assertTrue(task.is_scheduled())

    def test_check_rescheduled_after_execution(self):
        task = task_factory(TaskType.REPEATABLE, scheduled_time=timezone.now() + timedelta(seconds=1), repeat=10)
        queue = task.rqueue
        first_run_id = task.job_name
        entry = JobModel.get(first_run_id, connection=queue.connection)
        queue.run_sync(entry)
        task.refresh_from_db()
        self.assertEqual(task.failed_runs, 0)
        self.assertIsNone(task.last_failed_run)
        self.assertEqual(task.successful_runs, 1)
        self.assertIsNotNone(task.last_successful_run)
        self.assertTrue(task.is_scheduled())
        self.assertNotEqual(task.job_name, first_run_id)

    def test_check_rescheduled_after_execution_failed_job(self):
        task = task_factory(
            TaskType.REPEATABLE,
            callable_name="scheduler.tests.jobs.failing_job",
            scheduled_time=timezone.now() + timedelta(seconds=1),
            repeat=10,
        )
        queue = task.rqueue
        first_run_id = task.job_name
        entry = JobModel.get(first_run_id, connection=queue.connection)
        queue.run_sync(entry)
        task.refresh_from_db()
        self.assertEqual(task.failed_runs, 1)
        self.assertIsNotNone(task.last_failed_run)
        self.assertEqual(task.successful_runs, 0)
        self.assertIsNone(task.last_successful_run)
        self.assertTrue(task.is_scheduled())
        self.assertNotEqual(task.job_name, first_run_id)

    def test_check_not_rescheduled_after_last_repeat(self):
        task = task_factory(
            TaskType.REPEATABLE,
            scheduled_time=timezone.now() + timedelta(seconds=1),
            repeat=1,
        )
        queue = task.rqueue
        first_run_id = task.job_name
        entry = JobModel.get(first_run_id, connection=queue.connection)
        queue.run_sync(entry)
        task.refresh_from_db()
        self.assertEqual(task.failed_runs, 0)
        self.assertIsNone(task.last_failed_run)
        self.assertEqual(task.successful_runs, 1)
        self.assertIsNotNone(task.last_successful_run)
        self.assertNotEqual(task.job_name, first_run_id)
