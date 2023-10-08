from datetime import timedelta

from django.utils import timezone

from scheduler.models import ScheduledTask
from scheduler.tests.testtools import SchedulerBaseCase, job_factory
from scheduler.tools import get_scheduled_task


class TestInternals(SchedulerBaseCase):
    def test_get_scheduled_job(self):
        job = job_factory(ScheduledTask, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertEqual(job, get_scheduled_task(job.TASK_TYPE, job.id))
        with self.assertRaises(ValueError):
            get_scheduled_task(job.TASK_TYPE, job.id + 1)
        with self.assertRaises(ValueError):
            get_scheduled_task('UNKNOWN_JOBTYPE', job.id)
