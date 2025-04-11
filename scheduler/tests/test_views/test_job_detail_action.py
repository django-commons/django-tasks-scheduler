from django.urls import reverse

from scheduler.helpers.queues import get_queue
from scheduler.worker import create_worker
from scheduler.redis_models import JobStatus, JobModel
from scheduler.tests.jobs import failing_job, long_job, test_job
from scheduler.tests.testtools import assert_message_in_response
from .base import BaseTestCase
from ..test_task_types.test_task_model import assert_response_has_msg


class SingleJobActionViewsTest(BaseTestCase):
    def test_single_job_action_unknown_job(self):
        res = self.client.get(reverse("job_detail_action", args=["unknown", "cancel"]), follow=True)
        self.assertEqual(200, res.status_code)
        assert_response_has_msg(res, "Job unknown does not exist, maybe its TTL has passed")

    def test_single_job_action_unknown_action(self):
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(failing_job)
        worker = create_worker("default", burst=True)
        worker.work()
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(job.is_failed)
        res = self.client.get(reverse("job_detail_action", args=[job.name, "unknown"]), follow=True)
        self.assertEqual(400, res.status_code)

    def test_single_job_action_delete_job(self):
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job, job_info_ttl=0)
        res = self.client.get(reverse("job_detail_action", args=[job.name, "delete"]), follow=True)
        self.assertEqual(200, res.status_code)
        self.client.post(reverse("job_detail_action", args=[job.name, "delete"]), {"post": "yes"}, follow=True)
        self.assertFalse(JobModel.exists(job.name, connection=queue.connection))
        self.assertNotIn(job.name, queue.queued_job_registry.all())

    def test_single_job_action_cancel_job(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(long_job)
        self.assertTrue(job.is_queued)
        job = JobModel.get(job.name, connection=queue.connection)
        res = self.client.get(reverse("job_detail_action", args=[job.name, "cancel"]), follow=True)
        self.assertEqual(200, res.status_code)
        res = self.client.post(reverse("job_detail_action", args=[job.name, "cancel"]), {"post": "yes"}, follow=True)
        self.assertEqual(200, res.status_code)
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(job.is_canceled)
        self.assertNotIn(job.name, queue.queued_job_registry.all())

    def test_single_job_action_cancel_job_that_is_already_cancelled(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(long_job)
        res = self.client.post(reverse("job_detail_action", args=[job.name, "cancel"]), {"post": "yes"}, follow=True)
        self.assertEqual(200, res.status_code)
        tmp = JobModel.get(job.name, connection=queue.connection)
        self.assertTrue(tmp.is_canceled)
        self.assertNotIn(job.name, queue.queued_job_registry.all())
        res = self.client.post(reverse("job_detail_action", args=[job.name, "cancel"]), {"post": "yes"}, follow=True)
        self.assertEqual(200, res.status_code)
        assert_message_in_response(res, f"Could not perform action: Cannot cancel already canceled job: {job.name}")

    def test_single_job_action_enqueue_job(self):
        queue = get_queue("django_tasks_scheduler_test")
        job_list = []
        # enqueue some jobs that depends on other
        for _ in range(0, 3):
            job = queue.create_and_enqueue_job(test_job)
            job_list.append(job)

        self.assertEqual(job_list[-1].get_status(connection=queue.connection), JobStatus.QUEUED)
        self.assertIsNotNone(job_list[-1].enqueued_at)

        # Try to force enqueue last job should do nothing
        res = self.client.get(reverse("job_detail_action", args=[job_list[-1].name, "enqueue"]), follow=True)
        self.assertEqual(200, res.status_code)
        res = self.client.post(reverse("job_detail_action", args=[job_list[-1].name, "enqueue"]), follow=True)

        self.assertEqual(200, res.status_code)
        tmp = JobModel.get(job_list[-1].name, connection=queue.connection)
        self.assertEqual(tmp.get_status(connection=queue.connection), JobStatus.QUEUED)
        self.assertIsNotNone(tmp.enqueued_at)

    def test_single_job_action_enqueue_job_sync_queue(self):
        queue = get_queue("scheduler_scheduler_active_test")
        job_list = []
        # enqueue some jobs that depends on other
        for _ in range(0, 3):
            job = queue.create_and_enqueue_job(test_job)
            job_list.append(job)

        self.assertEqual(job_list[-1].status, JobStatus.FINISHED)
        self.assertIsNotNone(job_list[-1].enqueued_at)

        # Try to force enqueue last job should do nothing
        res = self.client.get(reverse("job_detail_action", args=[job_list[-1].name, "enqueue"]), follow=True)
        self.assertEqual(200, res.status_code)
        res = self.client.post(reverse("job_detail_action", args=[job_list[-1].name, "enqueue"]), follow=True)

        self.assertEqual(200, res.status_code)
        tmp = JobModel.get(job_list[-1].name, connection=queue.connection)
        self.assertEqual(tmp.get_status(connection=queue.connection), JobStatus.FINISHED)
        self.assertIsNotNone(tmp.enqueued_at)
        self.assertGreater(tmp.enqueued_at, job_list[-1].enqueued_at)
