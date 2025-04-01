from django.core.exceptions import ValidationError

from scheduler import settings
from scheduler.helpers.queues import get_queue
from scheduler.models import TaskType
from scheduler.redis_models import JobModel
from scheduler.tests.test_task_types.test_task_model import BaseTestCases
from scheduler.tests.testtools import task_factory
from scheduler.worker import create_worker


class TestCronTask(BaseTestCases.TestBaseTask):
    task_type = TaskType.CRON

    def setUp(self) -> None:
        super().setUp()
        self.queue_name = settings.get_queue_names()[0]

    def test_clean(self):
        task = task_factory(self.task_type)
        task.cron_string = "* * * * *"
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        self.assertIsNone(task.clean())

    def test_clean_cron_string_invalid(self):
        task = task_factory(self.task_type)
        task.cron_string = "not-a-cron-string"
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        with self.assertRaises(ValidationError):
            task.clean_cron_string()

    def test_check_rescheduled_after_execution(self):
        task = task_factory(self.task_type)
        queue = task.rqueue
        first_run_id = task.job_name
        entry = JobModel.get(first_run_id, connection=queue.connection)
        self.assertIsNotNone(entry)
        queue.run_sync(entry)
        task.refresh_from_db()
        self.assertEqual(task.failed_runs, 0)
        self.assertIsNone(task.last_failed_run)
        self.assertEqual(task.successful_runs, 1)
        self.assertIsNotNone(task.last_successful_run)
        self.assertTrue(task.is_scheduled())
        self.assertNotEqual(task.job_name, first_run_id)

    def test_check_rescheduled_after_failed_execution(self):
        task = task_factory(self.task_type, callable_name="scheduler.tests.jobs.failing_job")
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

    def test_cron_task_enqueuing_jobs(self):
        queue = get_queue()
        prev_queued = queue.scheduled_job_registry.count(connection=queue.connection)
        prev_finished = queue.finished_job_registry.count(connection=queue.connection)

        task = task_factory(self.task_type, callable_name="scheduler.tests.jobs.enqueue_jobs")
        self.assertEqual(prev_queued + 1, queue.scheduled_job_registry.count(connection=queue.connection))
        first_run_id = task.job_name
        entry = JobModel.get(first_run_id, connection=queue.connection)
        queue.run_sync(entry)
        self.assertEqual(20, len(queue.queued_job_registry))
        self.assertEqual(prev_finished + 1, queue.finished_job_registry.count(connection=queue.connection))
        worker = create_worker("default", fork_job_execution=False, burst=True)
        worker.work()
        self.assertEqual(prev_finished + 21, queue.finished_job_registry.count(connection=queue.connection))
        worker.refresh(update_queues=True)
        self.assertEqual(20, worker._model.successful_job_count)
        self.assertEqual(0, worker._model.failed_job_count)
