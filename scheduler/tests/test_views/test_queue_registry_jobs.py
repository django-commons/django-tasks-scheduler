import time
from datetime import datetime

from django.urls import reverse

from scheduler.helpers.queues import get_queue
from scheduler.tests.jobs import test_job
from scheduler.tests.test_views.base import BaseTestCase
from scheduler.tests.testtools import task_factory
from scheduler.models import TaskType, Task


class QueueRegistryJobsViewTest(BaseTestCase):
    def test_queue_jobs_unknown_registry(self):
        queue_name = "default"
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "unknown"]), follow=True)
        self.assertEqual(404, res.status_code)

    def test_queue_jobs_unknown_queue(self):
        res = self.client.get(reverse("queue_registry_jobs", args=["UNKNOWN", "queued"]))
        self.assertEqual(404, res.status_code)

    def test_queued_jobs(self):
        """Jobs in queue are displayed properly"""
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job)
        queue_name = "default"
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "queued"]))
        self.assertEqual(res.context["jobs"], [job])

    def test_finished_jobs(self):
        """Ensure that finished jobs page works properly."""
        queue = get_queue("django_tasks_scheduler_test")
        queue_name = "django_tasks_scheduler_test"

        job = queue.create_and_enqueue_job(test_job)
        registry = queue.finished_job_registry
        registry.add(queue.connection, job.name, time.time() + 2)
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "finished"]))
        self.assertEqual(res.context["jobs"], [job])

    def test_failed_jobs(self):
        """Ensure that failed jobs page works properly."""
        queue = get_queue("django_tasks_scheduler_test")
        queue_name = "django_tasks_scheduler_test"

        # Test that page doesn't fail when FailedJobRegistry is empty
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "failed"]))
        self.assertEqual(res.status_code, 200)

        job = queue.create_and_enqueue_job(test_job)
        registry = queue.failed_job_registry
        registry.add(queue.connection, job.name, time.time() + 20)
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "failed"]))
        self.assertEqual(res.context["jobs"], [job])

    def test_scheduled_jobs(self):
        """Ensure that scheduled jobs page works properly."""
        queue = get_queue("django_tasks_scheduler_test")
        queue_name = "django_tasks_scheduler_test"

        # Test that page doesn't fail when ScheduledJobRegistry is empty
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "scheduled"]))
        self.assertEqual(res.status_code, 200)

        job = queue.create_and_enqueue_job(test_job, when=datetime.now())
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "scheduled"]))
        self.assertEqual(res.context["jobs"], [job])

    def test_scheduled_jobs_registry_removal(self):
        """Ensure that non-existing job is being deleted from registry by view"""
        queue = get_queue("django_tasks_scheduler_test")
        queue_name = "django_tasks_scheduler_test"

        registry = queue.scheduled_job_registry
        job = queue.create_and_enqueue_job(test_job, when=datetime.now())
        self.assertEqual(registry.count(queue.connection), 1)

        queue.delete_job(job.name)
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "scheduled"]))
        self.assertEqual(res.context["jobs"], [])

        self.assertEqual(registry.count(queue.connection), 0)

    def test_started_jobs(self):
        """Ensure that active jobs page works properly."""
        queue = get_queue("django_tasks_scheduler_test")
        queue_name = "django_tasks_scheduler_test"

        job = queue.create_and_enqueue_job(test_job)
        registry = queue.active_job_registry
        registry.add(queue.connection, job.name, time.time() + 20)
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "active"]))
        self.assertEqual(res.context["jobs"], [job])

    def test_missing_task_doesnt_crash_job_detail_page(self):
        """
        Ensure that when a Task gets deleted and its Job doesn't get cleaned, the
        job detail page doesn't raise an exception.
        """
        queue_name = "django_tasks_scheduler_test"

        # No jobs in the queue
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "scheduled"]))
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.context["jobs"], [])

        task = task_factory(TaskType.ONCE, queue="django_tasks_scheduler_test")

        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "scheduled"]))
        self.assertEqual(res.status_code, 200)
        job = res.context["jobs"][0]
        self.assertTrue(job)
        self.assertTrue(job.is_scheduled_task)
        self.assertEqual(job.scheduled_task_id, task.pk)

        # Job detail page works
        url = reverse("job_details", args=[job.name])
        res = self.client.get(url)
        self.assertEqual(200, res.status_code)
        self.assertIn("job", res.context)
        self.assertEqual(res.context["job"], job)
        self.assertNotContains(res, "ValueError(&#x27;Invalid task type OnceTaskType&#x27;)")
        self.assertContains(res, "Link to scheduled job")

        # Delete all tasks in bulk, this doesn't trigger the signal
        # that would delete the corresponding scheduled jobs.
        Task.objects.all().delete()

        # The job lingers around :(
        res = self.client.get(reverse("queue_registry_jobs", args=[queue_name, "scheduled"]))
        self.assertEqual(res.status_code, 200)
        self.assertTrue(job in res.context["jobs"])

        # Job detail does't raise a 500
        url = reverse("job_details", args=[job.name])
        res = self.client.get(url)
        self.assertEqual(200, res.status_code)
        self.assertIn("job", res.context)
        self.assertEqual(res.context["job"], job)
        self.assertContains(res, "ValueError(&#x27;Invalid task type OnceTaskType&#x27;)")
        self.assertNotContains(res, "Link to scheduled job")
