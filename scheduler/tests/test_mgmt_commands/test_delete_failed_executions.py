from django.core.management import call_command

from scheduler.queues import get_queue
from scheduler.tests.jobs import failing_job
from scheduler.tests.test_views import BaseTestCase
from scheduler.tools import create_worker
from scheduler.tests import test_settings  # noqa


class DeleteFailedExecutionsTest(BaseTestCase):
    def test_delete_failed_executions__delete_jobs(self):
        queue = get_queue("default")
        call_command("delete_failed_executions", queue="default")
        queue.enqueue(failing_job)
        worker = create_worker("default")
        worker.work(burst=True)
        self.assertEqual(1, len(queue.failed_job_registry))
        call_command("delete_failed_executions", queue="default")
        self.assertEqual(0, len(queue.failed_job_registry))
