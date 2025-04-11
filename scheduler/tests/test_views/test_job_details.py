import uuid
from datetime import datetime
from unittest.mock import patch, PropertyMock

from django.urls import reverse

from scheduler.helpers.queues import get_queue
from scheduler.models import TaskType
from scheduler.redis_models import JobModel, WorkerModel
from scheduler.tests import test_settings  # noqa
from scheduler.tests.jobs import failing_job, test_job
from scheduler.tests.test_task_types.test_task_model import assert_response_has_msg
from scheduler.tests.test_views.base import BaseTestCase
from scheduler.tests.testtools import assert_message_in_response, task_factory, _get_task_scheduled_job_from_registry
from scheduler.types import QueueConfiguration
from scheduler.worker import create_worker


class TestViewJobDetails(BaseTestCase):
    def test_job_details(self):
        """Job data is displayed properly"""
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job)

        url = reverse("job_details", args=[job.name])
        res = self.client.get(url)
        self.assertEqual(200, res.status_code)
        self.assertIn("job", res.context)
        self.assertEqual(res.context["job"], job)

        # Bad job-id should return 404
        url = reverse("job_details", args=["bad_job_name"])
        res = self.client.get(url, follow=True)
        self.assertEqual(200, res.status_code)
        assert_response_has_msg(res, "Job bad_job_name does not exist, maybe its TTL has passed")

    def test_scheduled_job_details(self):
        """Job data is displayed properly"""
        scheduled_job = task_factory(TaskType.ONCE, enabled=True)
        job = _get_task_scheduled_job_from_registry(scheduled_job)

        url = reverse(
            "job_details",
            args=[
                job.name,
            ],
        )
        res = self.client.get(url, follow=True)
        self.assertIn("job", res.context)
        self.assertEqual(res.context["job"], job)

    def test_job_details_on_deleted_dependency(self):
        """Page doesn't crash even if job.dependency has been deleted"""
        queue = get_queue("default")

        job = queue.create_and_enqueue_job(test_job)
        second_job = queue.create_and_enqueue_job(test_job)
        queue.delete_job(job.name)
        url = reverse("job_details", args=[second_job.name])
        res = self.client.get(url)
        self.assertEqual(res.status_code, 200)

    def test_requeue_all(self):
        """Ensure that re-queuing all failed job work properly"""
        queue = get_queue("default")
        queue_name = "default"
        queue.create_and_enqueue_job(failing_job)
        queue.create_and_enqueue_job(failing_job)
        worker = create_worker("default", burst=True)
        worker.work()

        res = self.client.get(reverse("queue_registry_action", args=[queue_name, "failed", "requeue"]))
        self.assertEqual(res.context["total_jobs"], 2)
        # After requeue_all is called, jobs are enqueued
        res = self.client.post(reverse("queue_registry_action", args=[queue_name, "failed", "requeue"]))
        self.assertEqual(len(queue), 4)

    def test_requeue_all_if_deleted_job(self):
        """
        Ensure that re-queuing all failed job work properly
        """
        queue = get_queue("default")
        queue_name = "default"
        job = queue.create_and_enqueue_job(failing_job)
        queue.create_and_enqueue_job(failing_job)
        worker = create_worker("default", burst=True)
        worker.work()

        res = self.client.get(reverse("queue_registry_action", args=[queue_name, "failed", "requeue"]))
        self.assertEqual(res.context["total_jobs"], 2)
        queue.delete_job(job.name)

        # After requeue_all is called, jobs are enqueued
        res = self.client.post(reverse("queue_registry_action", args=[queue_name, "failed", "requeue"]))
        self.assertEqual(len(queue.queued_job_registry), 1)

    def test_clear_queue_unknown_registry(self):
        queue_name = "django_tasks_scheduler_test"
        res = self.client.post(reverse("queue_registry_action", args=[queue_name, "unknown", "empty"]), {"post": "yes"})
        self.assertEqual(404, res.status_code)

    def test_clear_queue_enqueued(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(test_job)
        self.client.post(reverse("queue_registry_action", args=[queue.name, "queued", "empty"]), {"post": "yes"})
        self.assertFalse(JobModel.exists(job.name, connection=queue.connection), f"job {job.name} exists")
        self.assertNotIn(job.name, queue.queued_job_registry.all())

    def test_clear_queue_scheduled(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(test_job, when=datetime.now())

        res = self.client.get(reverse("queue_registry_action", args=[queue.name, "scheduled", "empty"]), follow=True)
        self.assertEqual(200, res.status_code)
        self.assertEqual(res.context["jobs"], [job])

        res = self.client.post(
            reverse("queue_registry_action", args=[queue.name, "scheduled", "empty"]), {"post": "yes"}, follow=True
        )
        assert_message_in_response(res, f"You have successfully cleared the scheduled jobs in queue {queue.name}")
        self.assertEqual(200, res.status_code)
        self.assertFalse(JobModel.exists(job.name, connection=queue.connection))
        self.assertNotIn(job.name, queue.queued_job_registry.all())

    def test_queue_workers(self):
        """Worker index page should show workers for a specific queue"""
        queue_name = "django_tasks_scheduler_test"

        worker1 = create_worker(queue_name)
        worker1.worker_start()
        worker2 = create_worker("test3")
        worker2.worker_start()

        res = self.client.get(reverse("queue_workers", args=[queue_name]))
        worker1_model = WorkerModel.get(worker1.name, connection=worker1.connection)
        self.assertEqual(res.context["workers"], [worker1_model])

    def test_worker_details(self):
        """Worker index page should show workers for a specific queue"""

        worker = create_worker("django_tasks_scheduler_test", name=uuid.uuid4().hex)
        worker.worker_start()

        url = reverse("worker_details", args=[worker.name])
        res = self.client.get(url)
        self.assertEqual(res.context["worker"], worker._model)

    def test_worker_details__non_existing_worker(self):
        """Worker index page should show workers for a specific queue"""

        worker = create_worker("django_tasks_scheduler_test", name="WORKER")
        worker.worker_start()

        res = self.client.get(reverse("worker_details", args=["bad-worker-name"]))
        self.assertEqual(404, res.status_code)

    def test_statistics_json_view(self):
        # Override testing SCHEDULER_QUEUES
        queues = {
            "default": QueueConfiguration(DB=0, HOST="localhost", PORT=6379),
        }
        with patch("scheduler.settings._QUEUES", new_callable=PropertyMock(return_value=queues)):
            res = self.client.get(reverse("queues_home"))
            self.assertEqual(res.status_code, 200)

            res = self.client.get(reverse("queues_home_json"))
            self.assertEqual(res.status_code, 200)

            # Not staff => return 404
            self.user.is_staff = False
            self.user.save()

            res = self.client.get(reverse("queues_home"))
            self.assertEqual(res.status_code, 302)

            # 404 code for stats
            res = self.client.get(reverse("queues_home_json"))
            self.assertEqual(res.status_code, 404)

    @staticmethod
    def token_validation(token: str) -> bool:
        return token == "valid"

    # @patch('scheduler.views.SCHEDULER_CONFIG')
    # def test_statistics_json_view_token(self, configuration):
    #     configuration.get.return_value = ViewTest.token_validation
    #     self.user.is_staff = False
    #     self.user.save()
    #     res = self.client.get(reverse('queues_home_json'), headers={'Authorization': 'valid'})
    #     self.assertEqual(res.status_code, 200)
    #
    #     res = self.client.get(reverse('queues_home_json'), headers={'Authorization': 'invalid'})
    #     self.assertEqual(res.status_code, 404)
