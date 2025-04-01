from django.core.management import call_command

from scheduler.helpers.queues import get_queue
from scheduler.tests import test_settings  # noqa
from scheduler.tests.jobs import failing_job
from scheduler.worker import create_worker
from scheduler.tests.test_views.base import BaseTestCase


class DeleteFailedExecutionsTest(BaseTestCase):
    def test_delete_failed_executions__delete_jobs(self):
        queue = get_queue("default")
        call_command("delete_failed_executions", queue="default")
        queue.create_and_enqueue_job(failing_job)
        self.assertEqual(1, len(queue.queued_job_registry))
        worker = create_worker("default", burst=True)
        worker.work()
        self.assertEqual(1, len(queue.failed_job_registry))
        call_command("delete_failed_executions", queue="default")
        self.assertEqual(0, len(queue.failed_job_registry))
