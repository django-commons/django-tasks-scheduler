from unittest import mock

from django.core.management import call_command
from django.test import TestCase

from scheduler.helpers.queues import get_queue
from scheduler.redis_models import JobModel
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.tests import test_settings  # noqa
from scheduler.tests.jobs import failing_job


class SchedulerWorkerTestCase(TestCase):
    def test_scheduler_worker__no_queues_params(self):
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 1
        queue = get_queue("default")

        # enqueue some jobs that will fail
        job_names = []
        for _ in range(0, 3):
            job = queue.create_and_enqueue_job(failing_job)
            job_names.append(job.name)

        # Create a worker to execute these jobs
        call_command("scheduler_worker", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job_name in job_names:
            job = JobModel.get(name=job_name, connection=queue.connection)
            self.assertTrue(job.is_failed)
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 10

    @mock.patch("scheduler.management.commands.scheduler_worker.create_worker")
    def test_scheduler_worker__without_scheduler(self, mock_create_worker):
        # Create a worker to execute these jobs
        call_command("scheduler_worker", "default", "--burst", "--without-scheduler")
        mock_create_worker.assert_called_once_with(
            "default",
            name=None,
            fork_job_execution=True,
            burst=True,
            with_scheduler=False,
        )

    @mock.patch("scheduler.management.commands.scheduler_worker.create_worker")
    def test_scheduler_worker__with_scheduler(self, mock_create_worker):
        # Create a worker to execute these jobs
        call_command("scheduler_worker", "default", "--burst")
        mock_create_worker.assert_called_once_with(
            "default",
            name=None,
            fork_job_execution=True,
            burst=True,
            with_scheduler=True,
        )

    def test_scheduler_worker__run_jobs(self):
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 1
        queue = get_queue("default")

        # enqueue some jobs that will fail
        job_names = []
        for _ in range(0, 3):
            job = queue.create_and_enqueue_job(failing_job)
            job_names.append(job.name)

        # Create a worker to execute these jobs
        call_command("scheduler_worker", "default", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job_name in job_names:
            job = JobModel.get(name=job_name, connection=queue.connection)
            self.assertTrue(job.is_failed)
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 10

    def test_scheduler_worker__worker_with_two_queues(self):
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 1
        queue = get_queue("default")
        queue2 = get_queue("django_tasks_scheduler_test")

        # enqueue some jobs that will fail
        job_names = []
        for _ in range(0, 3):
            job = queue.create_and_enqueue_job(failing_job)
            job_names.append(job.name)
        job = queue2.create_and_enqueue_job(failing_job)
        job_names.append(job.name)

        # Create a worker to execute these jobs
        call_command("scheduler_worker", "default", "django_tasks_scheduler_test", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job_name in job_names:
            job = JobModel.get(name=job_name, connection=queue.connection)
            self.assertTrue(job.is_failed)
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 10

    def test_scheduler_worker__worker_with_one_queue__does_not_perform_other_queue_job(self):
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 1
        queue = get_queue("default")
        queue2 = get_queue("django_tasks_scheduler_test")

        job = queue.create_and_enqueue_job(failing_job)
        other_job = queue2.create_and_enqueue_job(failing_job)

        # Create a worker to execute these jobs
        call_command("scheduler_worker", "default", fork_job_execution=False, burst=True)

        # assert
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(job.is_failed)
        other_job = JobModel.get(other_job.name, connection=queue.connection)

        self.assertTrue(other_job.is_queued, f"Expected other job to be queued but status={other_job.status}")
        SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 10
