from datetime import timedelta

from django.utils import timezone

from scheduler.models import ScheduledTask
from scheduler.tests.testtools import SchedulerBaseCase, task_factory
from scheduler.tools import get_scheduled_task


class TestInternals(SchedulerBaseCase):
    def test_get_scheduled_job(self):
        task = task_factory(ScheduledTask, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertEqual(task, get_scheduled_task(task.TASK_TYPE, task.id))
        with self.assertRaises(ValueError):
            get_scheduled_task(task.TASK_TYPE, task.id + 1)
        with self.assertRaises(ValueError):
            get_scheduled_task('UNKNOWN_JOBTYPE', task.id)
