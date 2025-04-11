from django.core.management import call_command
from django.test import TestCase

from scheduler.helpers.queues import get_queue
from scheduler.redis_models import JobModel
from scheduler.tests import test_settings  # noqa
from scheduler.tests.jobs import test_job


class RunJobTest(TestCase):
    def test_run_job__should_schedule_job(self):
        queue = get_queue("default")
        queue.queued_job_registry.empty()
        func_name = f"{test_job.__module__}.{test_job.__name__}"
        # act
        call_command("run_job", func_name, queue="default")
        # assert
        job_list = JobModel.get_many(queue.queued_job_registry.all(), queue.connection)
        self.assertEqual(1, len(job_list))
        self.assertEqual(func_name + "()", job_list[0].get_call_string())
