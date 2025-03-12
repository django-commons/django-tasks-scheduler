from django.core.management import call_command
from django.test import TestCase

from scheduler.helpers.queues import get_queue
from scheduler.redis_models import JobModel
from scheduler.tests import test_settings  # noqa
from scheduler.tests.jobs import failing_job


class RqworkerTestCase(TestCase):

    def test_rqworker__no_queues_params(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        job_names = []
        for _ in range(0, 3):
            job = queue.enqueue_call(failing_job)
            job_names.append(job.name)

        # Create a worker to execute these jobs
        call_command("rqworker", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job_name in job_names:
            job = JobModel.get(name=job_name, connection=queue.connection)
            self.assertTrue(job.is_failed)

    def test_rqworker__run_jobs(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        job_names = []
        for _ in range(0, 3):
            job = queue.enqueue_call(failing_job)
            job_names.append(job.name)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job_name in job_names:
            job = JobModel.get(name=job_name, connection=queue.connection)
            self.assertTrue(job.is_failed)

    def test_rqworker__worker_with_two_queues(self):
        queue = get_queue("default")
        queue2 = get_queue("django_tasks_scheduler_test")

        # enqueue some jobs that will fail
        job_names = []
        for _ in range(0, 3):
            job = queue.enqueue_call(failing_job)
            job_names.append(job.name)
        job = queue2.enqueue_call(failing_job)
        job_names.append(job.name)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", "django_tasks_scheduler_test", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job_name in job_names:
            job = JobModel.get(name=job_name, connection=queue.connection)
            self.assertTrue(job.is_failed)

    def test_rqworker__worker_with_one_queue__does_not_perform_other_queue_job(self):
        queue = get_queue("default")
        queue2 = get_queue("django_tasks_scheduler_test")

        job = queue.enqueue_call(failing_job)
        other_job = queue2.enqueue_call(failing_job)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", fork_job_execution=False, burst=True)

        # assert
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(job.is_failed)
        other_job = JobModel.get(other_job.name, connection=queue.connection)

        self.assertTrue(other_job.is_queued, f"Expected other job to be queued but status={other_job.status}")
