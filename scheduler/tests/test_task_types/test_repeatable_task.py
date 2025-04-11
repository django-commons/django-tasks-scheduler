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
    task_type = TaskType.REPEATABLE
    queue_name = settings.get_queue_names()[0]

    def test_create_task_error(self):
        scheduled_time = timezone.now()

        Task.objects.create(
            name="konichiva_every_2s",
            callable="chat.task_scheduler.konichiva_func",
            task_type="REPEATABLE",
            interval=2,
            interval_unit="seconds",
            queue="default",
            enabled=True,
            scheduled_time=scheduled_time,
        )

    def test_unschedulable_old_job(self):
        job = task_factory(self.task_type, scheduled_time=timezone.now() - timedelta(hours=1), repeat=0)
        self.assertFalse(job.is_scheduled())

    def test_schedulable_old_job_repeat_none(self):
        # If repeat is None, the job should be scheduled
        job = task_factory(self.task_type, scheduled_time=timezone.now() - timedelta(hours=1), repeat=None)
        self.assertTrue(job.is_scheduled())

    def test_clean(self):
        job = task_factory(self.task_type)
        job.queue = self.queue_name
        job.callable = "scheduler.tests.jobs.test_job"
        job.interval = 1
        job.success_ttl = -1
        self.assertIsNone(job.clean())

    def test_clean_seconds(self):
        job = task_factory(self.task_type)
        job.queue = self.queue_name
        job.callable = "scheduler.tests.jobs.test_job"
        job.interval = 60
        job.success_ttl = -1
        job.interval_unit = "seconds"
        self.assertIsNone(job.clean())

    @override_settings(SCHEDULER_CONFIG=SchedulerConfiguration(SCHEDULER_INTERVAL=10))
    def test_clean_too_frequent(self):
        job = task_factory(self.task_type)
        job.queue = self.queue_name
        job.callable = "scheduler.tests.jobs.test_job"
        job.interval = 2  # Smaller than 10
        job.success_ttl = -1
        job.interval_unit = "seconds"
        with self.assertRaises(ValidationError):
            job.clean_interval_unit()

    @override_settings(SCHEDULER_CONFIG=SchedulerConfiguration(SCHEDULER_INTERVAL=10))
    def test_clean_not_multiple(self):
        job = task_factory(self.task_type)
        job.queue = self.queue_name
        job.callable = "scheduler.tests.jobs.test_job"
        job.interval = 121
        job.interval_unit = "seconds"
        with self.assertRaises(ValidationError):
            job.clean_interval_unit()

    def test_clean_short_result_ttl(self):
        task = task_factory(self.task_type)
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
        task = task_factory(self.task_type)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        task.interval = 1
        task.result_ttl = -1
        task.interval_unit = "hours"
        task.clean_result_ttl()

    def test_clean_undefined_result_ttl(self):
        task = task_factory(self.task_type)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        task.interval = 1
        task.interval_unit = "hours"
        task.clean_result_ttl()

    def test_interval_seconds_weeks(self):
        task = task_factory(self.task_type, interval=2, interval_unit="weeks")
        self.assertEqual(1209600.0, task.interval_seconds())

    def test_interval_seconds_days(self):
        task = task_factory(self.task_type, interval=2, interval_unit="days")
        self.assertEqual(172800.0, task.interval_seconds())

    def test_interval_seconds_hours(self):
        job = task_factory(self.task_type, interval=2, interval_unit="hours")
        self.assertEqual(7200.0, job.interval_seconds())

    def test_interval_seconds_minutes(self):
        job = task_factory(self.task_type, interval=15, interval_unit="minutes")
        self.assertEqual(900.0, job.interval_seconds())

    def test_interval_seconds_seconds(self):
        job = task_factory(self.task_type, interval=15, interval_unit="seconds")
        self.assertEqual(15.0, job.interval_seconds())

    def test_result_interval(self):
        job = task_factory(self.task_type)
        entry = _get_task_scheduled_job_from_registry(job)
        self.assertEqual(entry.meta["interval"], 3600)

    def test_repeat(self):
        job = task_factory(self.task_type, repeat=10)
        entry = _get_task_scheduled_job_from_registry(job)
        self.assertEqual(entry.meta["repeat"], 10)

    def test_repeat_old_job_exhausted(self):
        base_time = timezone.now()
        job = task_factory(self.task_type, scheduled_time=base_time - timedelta(hours=10), repeat=10)
        self.assertEqual(job.is_scheduled(), False)

    def test_repeat_old_job_last_iter(self):
        base_time = timezone.now()
        job = task_factory(self.task_type, scheduled_time=base_time - timedelta(hours=9, minutes=30), repeat=10)
        self.assertEqual(job.repeat, 0)
        self.assertEqual(job.is_scheduled(), True)

    def test_repeat_old_job_remaining(self):
        base_time = timezone.now()
        job = task_factory(self.task_type, scheduled_time=base_time - timedelta(minutes=30), repeat=5)
        self.assertEqual(job.repeat, 4)
        self.assertEqual(job.scheduled_time, base_time + timedelta(minutes=30))
        self.assertEqual(job.is_scheduled(), True)

    def test_repeat_none_interval_2_min(self):
        base_time = timezone.now()
        task = task_factory(self.task_type, scheduled_time=base_time - timedelta(minutes=29), repeat=None)
        task.interval = 120
        task.interval_unit = "seconds"
        task.save()
        self.assertTrue(task.scheduled_time > base_time)
        self.assertTrue(task.is_scheduled())

    def test_check_rescheduled_after_execution(self):
        task = task_factory(self.task_type, scheduled_time=timezone.now() + timedelta(seconds=1), repeat=10)
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
            self.task_type,
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
            self.task_type,
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
