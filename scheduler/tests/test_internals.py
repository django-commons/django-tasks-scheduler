from datetime import timedelta

from django.utils import timezone

from scheduler.models import ScheduledJob
from scheduler.tests.testtools import SchedulerBaseCase, job_factory
from scheduler.tools import get_scheduled_job


class TestInternals(SchedulerBaseCase):
    def test_get_scheduled_job(self):
        job = job_factory(ScheduledJob, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertEqual(job, get_scheduled_job(job.JOB_TYPE, job.id))
        with self.assertRaises(ValueError):
            get_scheduled_job(job.JOB_TYPE, job.id + 1)
        with self.assertRaises(ValueError):
            get_scheduled_job('UNKNOWN_JOBTYPE', job.id)
