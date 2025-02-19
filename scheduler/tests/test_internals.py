from datetime import timedelta

from django.utils import timezone

from scheduler.models.task import TaskType
from scheduler.tests.testtools import SchedulerBaseCase, task_factory
from scheduler.tools import get_scheduled_task


class TestInternals(SchedulerBaseCase):
    def test_get_scheduled_job(self):
        task = task_factory(TaskType.ONCE, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertEqual(task, get_scheduled_task(TaskType.ONCE, task.id))
        with self.assertRaises(ValueError):
            get_scheduled_task(task.task_type, task.id + 1)
        with self.assertRaises(ValueError):
            get_scheduled_task("UNKNOWN_JOBTYPE", task.id)
