from time import sleep

from django.test import tag
from django.urls import reverse

from scheduler.helpers.queues import get_queue
from scheduler.redis_models import JobStatus, JobModel, WorkerModel
from scheduler.tests.jobs import long_job
from .. import testtools
from ..test_views.base import BaseTestCase


@tag("multiprocess")
class MultiProcessTest(BaseTestCase):
    def test_cancel_job_after_it_started(self):
        # arrange
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(long_job)
        self.assertTrue(job.is_queued)
        process, worker_name = testtools.run_worker_in_process("django_tasks_scheduler_test")
        sleep(0.2)
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertEqual(JobStatus.STARTED, job.status)
        # act
        res = self.client.post(reverse("job_detail_action", args=[job.name, "cancel"]), {"post": "yes"}, follow=True)

        # assert
        self.assertEqual(200, res.status_code)
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertEqual(JobStatus.STOPPED, job.status)
        self.assertNotIn(job.name, queue.queued_job_registry.all())
        sleep(0.2)
        process.terminate()
        process.join(2)
        process.kill()
        worker_model = WorkerModel.get(worker_name, connection=queue.connection)
        self.assertEqual(0, worker_model.completed_jobs)
        self.assertEqual(0, worker_model.failed_job_count)
        self.assertEqual(0, worker_model.successful_job_count)
        self.assertIsNotNone(worker_model.shutdown_requested_date)
