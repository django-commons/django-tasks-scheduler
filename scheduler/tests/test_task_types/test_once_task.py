from datetime import timedelta

from django.utils import timezone

from scheduler import settings
from scheduler.models.task import TaskType
from scheduler.tests.test_task_types.test_task_model import BaseTestCases
from scheduler.tests.testtools import task_factory


class TestScheduledTask(BaseTestCases.TestSchedulableTask):
    task_type = TaskType.ONCE
    queue_name = settings.get_queue_names()[0]

    def test_clean(self):
        job = task_factory(self.task_type)
        job.queue = self.queue_name
        job.callable = "scheduler.tests.jobs.test_job"
        self.assertIsNone(job.clean())

    def test_unschedulable_old_job(self):
        job = task_factory(self.task_type, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertFalse(job.is_scheduled())
