from django.core.management import call_command

from scheduler.helpers.queues import get_queue
from scheduler.helpers.utils import current_timestamp
from scheduler.tests import conf  # noqa
from scheduler.tests.jobs import failing_job, test_job
from scheduler.tests.test_views.base import BaseTestCase
from scheduler.worker import create_worker


class DeleteFailedExecutionsTest(BaseTestCase):
    def test_delete_failed_executions__delete_jobs(self):
        queue = get_queue("default")
        call_command("delete_failed_executions", queue="default")
        queue.create_and_enqueue_job(failing_job)
        self.assertEqual(1, queue.queued_job_registry.count(queue.connection))
        worker = create_worker("default", burst=True)
        worker.work()
        self.assertEqual(1, queue.failed_job_registry.count(queue.connection))
        call_command("delete_failed_executions", queue="default")
        self.assertEqual(0, queue.failed_job_registry.count(queue.connection))

    def test_delete_failed_executions__func__deletes_only_its_jobs(self):
        queue = get_queue("default")
        failing = queue.create_and_enqueue_job(failing_job)
        other = queue.create_and_enqueue_job(test_job)
        for job in (failing, other):
            queue.failed_job_registry.add(queue.connection, job.name, current_timestamp() + 1000)

        call_command("delete_failed_executions", queue="default", func="scheduler.tests.jobs.failing_job")

        self.assertEqual([other.name], queue.failed_job_registry.all(queue.connection))
