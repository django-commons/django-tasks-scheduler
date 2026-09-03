from django.urls import reverse

from scheduler.helpers.queues import get_queue
from scheduler.redis_models import JobModel, JobStatus
from scheduler.tests.jobs import failing_job, test_job
from scheduler.tests.test_views.base import BaseTestCase
from scheduler.tests.testtools import assert_message_in_response
from scheduler.worker import create_worker


class QueueActionsViewsTest(BaseTestCase):
    def test_job_list_action_delete_jobs__with_bad_next_url(self):
        queue = get_queue("django_tasks_scheduler_test")

        # enqueue some jobs
        job_names = []
        for _ in range(3):
            job = queue.create_and_enqueue_job(test_job, job_info_ttl=0)
            job_names.append(job.name)

        # remove those jobs using view
        res = self.client.post(
            reverse("queue_job_actions", args=[queue.name]),
            {"action": "delete", "job_names": job_names, "next_url": "bad_url"},
            follow=True,
        )
        assert_message_in_response(res, "Bad followup URL")
        # check if jobs are removed
        self.assertEqual(200, res.status_code)
        for job_name in job_names:
            self.assertFalse(JobModel.exists(job_name, connection=queue.connection), f"job {job_name} exists")
            self.assertNotIn(job_name, queue.queued_job_registry.all(queue.connection))

    def test_job_list_action_delete_jobs(self):
        queue = get_queue("django_tasks_scheduler_test")

        # enqueue some jobs
        job_names = []
        for _ in range(3):
            job = queue.create_and_enqueue_job(test_job, job_info_ttl=0)
            job_names.append(job.name)

        # remove those jobs using view
        res = self.client.post(
            reverse("queue_job_actions", args=[queue.name]), {"action": "delete", "job_names": job_names}, follow=True
        )

        # check if jobs are removed
        self.assertEqual(200, res.status_code)
        for job_name in job_names:
            self.assertFalse(JobModel.exists(job_name, connection=queue.connection), f"job {job_name} exists")
            self.assertNotIn(job_name, queue.queued_job_registry.all(queue.connection))

    def test_job_list_action_requeue_jobs(self):
        queue_name = "django_tasks_scheduler_test"
        queue = get_queue(queue_name)

        # enqueue some jobs that will fail
        job_names = []
        for _ in range(3):
            job = queue.create_and_enqueue_job(failing_job)
            job_names.append(job.name)

        # do those jobs = fail them
        worker = create_worker(queue_name, burst=True)
        worker.work()

        # check if all jobs are really failed
        for job_name in job_names:
            job = JobModel.get(job_name, connection=queue.connection)
            self.assertTrue(job.is_failed)

        # re-nqueue failed jobs from failed queue
        self.client.post(reverse("queue_job_actions", args=[queue_name]), {"action": "requeue", "job_names": job_names})

        # check if we requeue all failed jobs
        for job_name in job_names:
            job = JobModel.get(job_name, connection=queue.connection)
            self.assertFalse(job.is_failed)

    def test_job_list_action_stop_jobs__move_to_finished_registry(self):
        queue_name = "django_tasks_scheduler_test"
        queue = get_queue(queue_name)

        # Enqueue some jobs
        job_names = []
        worker = create_worker(queue_name)
        worker.bootstrap()
        for _ in range(3):
            job = queue.create_and_enqueue_job(test_job)
            job_names.append(job.name)
            worker.worker_before_execution(job, connection=queue.connection)
            job.prepare_for_execution(worker.name, queue.active_job_registry, connection=queue.connection)

        # Check if the jobs are started
        for job_name in job_names:
            job = JobModel.get(job_name, connection=queue.connection)
            self.assertEqual(job.status, JobStatus.STARTED)

        # Stop those jobs using the view
        self.assertEqual(queue.active_job_registry.count(queue.connection), len(job_names))
        self.client.post(reverse("queue_job_actions", args=[queue_name]), {"action": "stop", "job_names": job_names})
        self.assertEqual(0, queue.active_job_registry.count(queue.connection))

        self.assertEqual(0, queue.canceled_job_registry.count(queue.connection))
        self.assertEqual(len(job_names), queue.finished_job_registry.count(queue.connection))

        for job_name in job_names:
            self.assertTrue(queue.finished_job_registry.exists(queue.connection, job_name))


class QueueConfirmJobActionViewTest(BaseTestCase):
    """`action in QueueJobAction` raised `TypeError` on python < 3.12.

    `EnumType.__contains__` only started accepting non-member values in python 3.12; before that it
    raised, so posting an action to the confirmation view returned a 500 on python 3.11. The enum's
    own `__contains__` did not help -- it was an instance method, and the check is on the class.
    """

    def test_confirm_job_action__known_action_renders(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(test_job)

        res = self.client.post(
            reverse("queue_confirm_job_action", args=[queue.name]),
            {"action": "delete", "_selected_action": [job.name]},
        )

        self.assertEqual(200, res.status_code)

    def test_confirm_job_action__unknown_action_redirects(self):
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(test_job)

        res = self.client.post(
            reverse("queue_confirm_job_action", args=[queue.name]),
            {"action": "not-an-action", "_selected_action": [job.name]},
        )

        self.assertEqual(302, res.status_code)
