from django.core.management import call_command
from django.test import TestCase

from scheduler.queues import get_queue
from scheduler.tests.jobs import failing_job
from scheduler.tests import test_settings  # noqa


class RqworkerTestCase(TestCase):
    def test_rqworker__no_queues_params(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # Create a worker to execute these jobs
        call_command("rqworker", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

    def test_rqworker__job_class_param__green(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # Create a worker to execute these jobs
        call_command(
            "rqworker", "--job-class", "scheduler.rq_classes.JobExecution", fork_job_execution=False, burst=True
        )

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

    def test_rqworker__bad_job_class__fail(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # Create a worker to execute these jobs
        with self.assertRaises(ImportError):
            call_command("rqworker", "--job-class", "rq.badclass", fork_job_execution=False, burst=True)

    def test_rqworker__run_jobs(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

    def test_rqworker__worker_with_two_queues(self):
        queue = get_queue("default")
        queue2 = get_queue("django_tasks_scheduler_test")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)
        job = queue2.enqueue(failing_job)
        jobs.append(job)
        job_ids.append(job.id)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", "django_tasks_scheduler_test", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

    def test_rqworker__worker_with_one_queue__does_not_perform_other_queue_job(self):
        queue = get_queue("default")
        queue2 = get_queue("django_tasks_scheduler_test")

        job = queue.enqueue(failing_job)
        other_job = queue2.enqueue(failing_job)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", fork_job_execution=False, burst=True)
        # assert
        self.assertTrue(job.is_failed)
        self.assertTrue(other_job.is_queued)
