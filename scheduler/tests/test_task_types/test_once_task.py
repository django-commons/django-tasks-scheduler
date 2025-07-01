from datetime import timedelta, datetime

import time_machine
from django.core.exceptions import ValidationError
from django.urls import reverse
from django.utils import timezone

from scheduler import settings
from scheduler.helpers.queues import get_queue
from scheduler.models import TaskType
from scheduler.redis_models import JobModel
from scheduler.tests.test_task_types.test_task_model import BaseTestCases
from scheduler.tests.testtools import task_factory


class TestScheduledOnceTask(BaseTestCases.TestSchedulableTask):
    task_type = TaskType.ONCE
    queue_name = settings.get_queue_names()[0]

    def test_clean(self):
        task = task_factory(self.task_type)
        task.queue = self.queue_name
        task.callable = "scheduler.tests.jobs.test_job"
        self.assertIsNone(task.clean())

    @time_machine.travel(datetime(2016, 12, 25))
    def test_admin_changelist_view__has_timezone_data(self):
        # arrange
        self.client.login(username="admin", password="admin")
        task_factory(self.task_type)
        url = reverse("admin:scheduler_task_changelist")
        # act
        res = self.client.get(url)
        # assert
        self.assertContains(res, "Run once: Dec. 26, 2016, midnight", count=1, status_code=200)

    @time_machine.travel(datetime(2016, 12, 25))
    def test_admin_change_view__has_execution_list(self):
        # arrange
        self.client.login(username="admin", password="admin")
        task = task_factory(self.task_type)
        url = reverse("admin:scheduler_task_change", args=(task.id,))
        # act
        res = self.client.get(url)
        # assert
        self.assertContains(res, "Job executions")
        self.assertContains(res, """<table id="result_list">""")
        self.assertContains(res, task.job_name, status_code=200)
        self.assertContains(res, "Scheduled", status_code=200)
        self.assertContains(res, """<span id="counter">1""", status_code=200)
        self.assertFalse(res.context["pagination_required"])
        self.assertEqual(res.context["executions"].paginator.count, 1)

    @time_machine.travel(datetime(2016, 12, 25))
    def test_admin_change_view__has_empty_execution_list(self):
        # arrange
        self.client.login(username="admin", password="admin")
        task = task_factory(self.task_type)
        queue = get_queue(self.queue_name)
        job = JobModel.get(task.job_name, connection=queue.connection)
        JobModel.delete(job, connection=queue.connection)
        url = reverse("admin:scheduler_task_change", args=(task.id,))
        # act
        res = self.client.get(url)
        # assert
        self.assertContains(res, "Job executions")
        self.assertContains(res, """<table id="result_list">""")
        self.assertContains(res, """<span id="counter">0""", status_code=200)
        self.assertFalse(res.context["pagination_required"])
        self.assertEqual(res.context["executions"].paginator.count, 0)

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
        task = task_factory(self.task_type, scheduled_time=timezone.now() - timedelta(hours=1), instance_only=True)
        task.save(clean=False)
        self.assertFalse(task.is_scheduled())
