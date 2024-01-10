from datetime import timedelta

from django.core.exceptions import ValidationError
from django.test import override_settings
from django.utils import timezone

from scheduler import settings
from scheduler.models import RepeatableTask
from scheduler.tests.test_models import BaseTestCases
from .testtools import (
    task_factory, _get_job_from_scheduled_registry)


class TestRepeatableTask(BaseTestCases.TestSchedulableJob):
    TaskModelClass = RepeatableTask

    def test_unschedulable_old_job(self):
        job = task_factory(self.TaskModelClass, scheduled_time=timezone.now() - timedelta(hours=1), repeat=0)
        self.assertFalse(job.is_scheduled())

    def test_schedulable_old_job_repeat_none(self):
        # If repeat is None, the job should be scheduled
        job = task_factory(self.TaskModelClass, scheduled_time=timezone.now() - timedelta(hours=1), repeat=None)
        self.assertTrue(job.is_scheduled())

    def test_clean(self):
        job = task_factory(self.TaskModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 1
        job.result_ttl = -1
        self.assertIsNone(job.clean())

    def test_clean_seconds(self):
        job = task_factory(self.TaskModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 60
        job.result_ttl = -1
        job.interval_unit = 'seconds'
        self.assertIsNone(job.clean())

    @override_settings(SCHEDULER_CONFIG={
        'SCHEDULER_INTERVAL': 10,
    })
    def test_clean_too_frequent(self):
        job = task_factory(self.TaskModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 2  # Smaller than 10
        job.result_ttl = -1
        job.interval_unit = 'seconds'
        with self.assertRaises(ValidationError):
            job.clean_interval_unit()

    def test_clean_not_multiple(self):
        job = task_factory(self.TaskModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 121
        job.interval_unit = 'seconds'
        with self.assertRaises(ValidationError):
            job.clean_interval_unit()

    def test_clean_short_result_ttl(self):
        job = task_factory(self.TaskModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 1
        job.repeat = 1
        job.result_ttl = 3599
        job.interval_unit = 'hours'
        job.repeat = 42
        with self.assertRaises(ValidationError):
            job.clean_result_ttl()

    def test_clean_indefinite_result_ttl(self):
        job = task_factory(self.TaskModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 1
        job.result_ttl = -1
        job.interval_unit = 'hours'
        job.clean_result_ttl()

    def test_clean_undefined_result_ttl(self):
        job = task_factory(self.TaskModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 1
        job.interval_unit = 'hours'
        job.clean_result_ttl()

    def test_interval_seconds_weeks(self):
        job = task_factory(self.TaskModelClass, interval=2, interval_unit='weeks')
        self.assertEqual(1209600.0, job.interval_seconds())

    def test_interval_seconds_days(self):
        job = task_factory(self.TaskModelClass, interval=2, interval_unit='days')
        self.assertEqual(172800.0, job.interval_seconds())

    def test_interval_seconds_hours(self):
        job = task_factory(self.TaskModelClass, interval=2, interval_unit='hours')
        self.assertEqual(7200.0, job.interval_seconds())

    def test_interval_seconds_minutes(self):
        job = task_factory(self.TaskModelClass, interval=15, interval_unit='minutes')
        self.assertEqual(900.0, job.interval_seconds())

    def test_interval_seconds_seconds(self):
        job = RepeatableTask(interval=15, interval_unit='seconds')
        self.assertEqual(15.0, job.interval_seconds())

    def test_interval_display(self):
        job = task_factory(self.TaskModelClass, interval=15, interval_unit='minutes')
        self.assertEqual(job.interval_display(), '15 minutes')

    def test_result_interval(self):
        job = task_factory(self.TaskModelClass, )
        entry = _get_job_from_scheduled_registry(job)
        self.assertEqual(entry.meta['interval'], 3600)

    def test_repeat(self):
        job = task_factory(self.TaskModelClass, repeat=10)
        entry = _get_job_from_scheduled_registry(job)
        self.assertEqual(entry.meta['repeat'], 10)

    def test_repeat_old_job_exhausted(self):
        base_time = timezone.now()
        job = task_factory(self.TaskModelClass, scheduled_time=base_time - timedelta(hours=10), repeat=10)
        self.assertEqual(job.is_scheduled(), False)

    def test_repeat_old_job_last_iter(self):
        base_time = timezone.now()
        job = task_factory(self.TaskModelClass, scheduled_time=base_time - timedelta(hours=9, minutes=30), repeat=10)
        self.assertEqual(job.repeat, 0)
        self.assertEqual(job.is_scheduled(), True)

    def test_repeat_old_job_remaining(self):
        base_time = timezone.now()
        job = task_factory(self.TaskModelClass, scheduled_time=base_time - timedelta(minutes=30), repeat=5)
        self.assertEqual(job.repeat, 4)
        self.assertEqual(job.scheduled_time, base_time + timedelta(minutes=30))
        self.assertEqual(job.is_scheduled(), True)

    def test_repeat_none_interval_2_min(self):
        base_time = timezone.now()
        job = task_factory(self.TaskModelClass, scheduled_time=base_time - timedelta(minutes=29), repeat=None)
        job.interval = 120
        job.interval_unit = 'seconds'
        job.schedule()
        self.assertTrue(job.scheduled_time > base_time)
        self.assertTrue(job.is_scheduled())

    def test_check_rescheduled_after_execution(self):
        task = task_factory(self.TaskModelClass, scheduled_time=timezone.now() + timedelta(seconds=1), repeat=10)
        queue = task.rqueue
        first_run_id = task.job_id
        entry = queue.fetch_job(first_run_id)
        queue.run_sync(entry)
        task.refresh_from_db()
        self.assertEqual(task.failed_runs, 0)
        self.assertIsNone(task.last_failed_run)
        self.assertEqual(task.successful_runs, 1)
        self.assertIsNotNone(task.last_successful_run)
        self.assertTrue(task.is_scheduled())
        self.assertNotEqual(task.job_id, first_run_id)

    def test_check_rescheduled_after_execution_failed_job(self):
        task = task_factory(
            self.TaskModelClass, callable_name='scheduler.tests.jobs.failing_job',
            scheduled_time=timezone.now() + timedelta(seconds=1),
            repeat=10, )
        queue = task.rqueue
        first_run_id = task.job_id
        entry = queue.fetch_job(first_run_id)
        queue.run_sync(entry)
        task.refresh_from_db()
        self.assertEqual(task.failed_runs, 1)
        self.assertIsNotNone(task.last_failed_run)
        self.assertEqual(task.successful_runs, 0)
        self.assertIsNone(task.last_successful_run)
        self.assertTrue(task.is_scheduled())
        self.assertNotEqual(task.job_id, first_run_id)

    def test_check_not_rescheduled_after_last_repeat(self):
        task = task_factory(
            self.TaskModelClass,
            scheduled_time=timezone.now() + timedelta(seconds=1),
            repeat=1,
        )
        queue = task.rqueue
        first_run_id = task.job_id
        entry = queue.fetch_job(first_run_id)
        queue.run_sync(entry)
        task.refresh_from_db()
        self.assertEqual(task.failed_runs, 0)
        self.assertIsNone(task.last_failed_run)
        self.assertEqual(task.successful_runs, 1)
        self.assertIsNotNone(task.last_successful_run)
        self.assertNotEqual(task.job_id, first_run_id)
