from django.urls import reverse

from scheduler.helpers.queues import get_queue
from scheduler.helpers.tools import create_worker
from scheduler.redis_models import JobStatus, JobModel
from scheduler.tests.jobs import failing_job, long_job, test_job
from scheduler.tests.testtools import assert_message_in_response
from .base import BaseTestCase


class SingleJobActionViewsTest(BaseTestCase):

    def test_single_job_action_unknown_job(self):
        res = self.client.get(reverse("queue_job_action", args=["unknown", "cancel"]), follow=True)
        self.assertEqual(400, res.status_code)

    def test_single_job_action_unknown_action(self):
        queue = get_queue("default")
        job = queue.enqueue_call(failing_job)
        worker = create_worker("default")
        worker.work(burst=True)
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(job.is_failed)
        res = self.client.get(reverse("queue_job_action", args=[job.name, "unknown"]), follow=True)
        self.assertEqual(404, res.status_code)

    def test_single_job_action_requeue_job(self):
        queue = get_queue("default")
        job = queue.enqueue_call(failing_job)
        worker = create_worker("default")
        worker.work(burst=True)
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(job.is_failed)
        res = self.client.get(reverse("queue_job_action", args=[job.name, "requeue"]), follow=True)
        self.assertEqual(200, res.status_code)
        self.client.post(reverse("queue_job_action", args=[job.name, "requeue"]), {"requeue": "Requeue"}, follow=True)
        self.assertIn(job, JobModel.get_many(queue.queued_job_registry.all(), queue.connection))
        queue.delete_job(job.name)

    def test_single_job_action_delete_job(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.enqueue_call(test_job)
        res = self.client.get(reverse("queue_job_action", args=[job.name, "delete"]), follow=True)
        self.assertEqual(200, res.status_code)
        self.client.post(reverse("queue_job_action", args=[job.name, "delete"]), {"post": "yes"}, follow=True)
        self.assertFalse(JobModel.exists(job.name, connection=queue.connection))
        self.assertNotIn(job.name, queue.queued_job_registry.all())

    def test_single_job_action_cancel_job(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.enqueue_call(long_job)
        res = self.client.get(reverse("queue_job_action", args=[job.name, "cancel"]), follow=True)
        self.assertEqual(200, res.status_code)
        res = self.client.post(reverse("queue_job_action", args=[job.name, "cancel"]), {"post": "yes"}, follow=True)
        self.assertEqual(200, res.status_code)
        tmp = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(tmp.is_canceled)
        self.assertNotIn(job.name, queue.queued_job_registry.all())

    def test_single_job_action_cancel_job_that_is_already_cancelled(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.enqueue_call(long_job)
        res = self.client.post(reverse("queue_job_action", args=[job.name, "cancel"]), {"post": "yes"}, follow=True)
        self.assertEqual(200, res.status_code)
        tmp = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(tmp.is_canceled)
        self.assertNotIn(job.name, queue.queued_job_registry.all())
        res = self.client.post(reverse("queue_job_action", args=[job.name, "cancel"]), {"post": "yes"}, follow=True)
        self.assertEqual(200, res.status_code)
        assert_message_in_response(res, f"Could not perform action: Cannot cancel already canceled job: {job.name}")

    def test_single_job_action_enqueue_job(self):
        queue = get_queue("django_tasks_scheduler_test")
        job_list = []
        # enqueue some jobs that depends on other
        previous_job = None
        for _ in range(0, 3):
            job = queue.enqueue_call(test_job)
            job_list.append(job)
            previous_job = job

        # This job is deferred

        self.assertEqual(job_list[-1].get_status(connection=queue.connection), JobStatus.QUEUED)
        self.assertIsNotNone(job_list[-1].enqueued_at)

        # Try to force enqueue last job should do nothing
        res = self.client.get(reverse("queue_job_action", args=[job_list[-1].name, "enqueue"]), follow=True)
        self.assertEqual(200, res.status_code)
        res = self.client.post(reverse("queue_job_action", args=[job_list[-1].name, "enqueue"]), follow=True)

        # Check that job is still deferred because it has dependencies (rq 1.14 change)
        self.assertEqual(200, res.status_code)
        tmp = JobModel.get(job_list[-1].name, connection=queue.connection)
        self.assertEqual(tmp.get_status(connection=queue.connection), JobStatus.QUEUED)
        self.assertIsNotNone(tmp.enqueued_at)
