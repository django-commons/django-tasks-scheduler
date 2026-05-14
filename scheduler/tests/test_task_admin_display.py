from django.contrib import admin
from django.test import TestCase

from scheduler.admin.task_admin import TaskAdmin
from scheduler.models import Task, TaskType
from scheduler.tests.testtools import task_factory


class TestTaskAdminNextRunDisplay(TestCase):
    def setUp(self) -> None:
        self.admin = TaskAdmin(Task, admin.site)

    def test_next_run_returns_not_scheduled_when_scheduled_time_is_none(self) -> None:
        # Regression: TypeError when scheduled_time is None (issue #363)
        task = task_factory(TaskType.CRON, enabled=False)
        task.scheduled_time = None
        task.save(clean=False)

        result = self.admin.next_run(task)

        assert str(result) == "Not scheduled"
