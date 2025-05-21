from datetime import timedelta, datetime

from django.core.exceptions import ValidationError
from django.utils import timezone

from scheduler import settings
from scheduler.models import TaskType
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

    def test_create_without_date__fail(self):
        task = task_factory(self.task_type, scheduled_time=None, instance_only=True)
        self.assertIsNone(task.scheduled_time)
        with self.assertRaises(Exception) as cm:
            task.clean()
        self.assertTrue(isinstance(cm.exception, ValidationError))
        self.assertEqual(str(cm.exception), "{'scheduled_time': ['Scheduled time is required']}")

    def test_create_with_date_in_the_past__fail(self):
        task = task_factory(self.task_type, scheduled_time=datetime.now() - timedelta(days=1), instance_only=True)
        with self.assertRaises(Exception) as cm:
            task.clean()
        self.assertTrue(isinstance(cm.exception, ValidationError))
        self.assertEqual(str(cm.exception), "{'scheduled_time': ['Scheduled time must be in the future']}")

    def test_unschedulable_old_job(self):
        job = task_factory(self.task_type, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertFalse(job.is_scheduled())
