from django.contrib import admin

from scheduler.admin.task_admin import TaskAdmin
from scheduler.models import Task, TaskType
from scheduler.tests.testtools import SchedulerBaseCase, task_factory


class TestTaskAdminNextRun(SchedulerBaseCase):
    def setUp(self):
        super().setUp()
        self.admin = TaskAdmin(Task, admin.site)

    def test_next_run_with_scheduled_time_none(self):
        task = task_factory(TaskType.CRON)
        task.scheduled_time = None
        self.assertEqual(str(self.admin.next_run(task)), "Not scheduled")

    def test_next_run_with_scheduled_time_set(self):
        task = task_factory(TaskType.ONCE)
        self.assertEqual(self.admin.next_run(task), task.scheduled_time)
