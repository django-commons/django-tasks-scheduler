from django.core.exceptions import ValidationError

from scheduler import settings
from scheduler.models import CronTask
from scheduler.tools import create_worker
from .test_models import BaseTestCases
from .testtools import task_factory
from ..queues import get_queue


class TestCronTask(BaseTestCases.TestBaseTask):
    TaskModelClass = CronTask

    def test_clean(self):
        task = task_factory(CronTask)
        task.cron_string = '* * * * *'
        task.queue = list(settings.QUEUES)[0]
        task.callable = 'scheduler.tests.jobs.test_job'
        self.assertIsNone(task.clean())

    def test_clean_cron_string_invalid(self):
        task = task_factory(CronTask)
        task.cron_string = 'not-a-cron-string'
        task.queue = list(settings.QUEUES)[0]
        task.callable = 'scheduler.tests.jobs.test_job'
        with self.assertRaises(ValidationError):
            task.clean_cron_string()

    def test_check_rescheduled_after_execution(self):
        task = task_factory(CronTask, )
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

    def test_check_rescheduled_after_failed_execution(self):
        task = task_factory(
            CronTask,
            callable_name="scheduler.tests.jobs.scheduler.tests.jobs.test_job",
        )
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

    def test_cron_task_enqueuing_jobs(self):
        queue = get_queue()
        prev_queued = len(queue.scheduled_job_registry)
        prev_finished = len(queue.finished_job_registry)
        task = task_factory(CronTask, callable_name='scheduler.tests.jobs.enqueue_jobs')
        self.assertEqual(prev_queued + 1, len(queue.scheduled_job_registry))
        first_run_id = task.job_id
        entry = queue.fetch_job(first_run_id)
        queue.run_sync(entry)
        self.assertEqual(20, len(queue))
        self.assertEqual(prev_finished + 1, len(queue.finished_job_registry))
        worker = create_worker('default', fork_job_execution=False, )
        worker.work(burst=True)
        self.assertEqual(prev_finished + 21, len(queue.finished_job_registry))
        worker.refresh()
        self.assertEqual(20, worker.successful_job_count)
        self.assertEqual(0, worker.failed_job_count)
