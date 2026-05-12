from datetime import timedelta

from django.contrib import admin
from django.test import TestCase
from django.utils import timezone

from scheduler.admin.task_admin import TaskAdmin
from scheduler.models import Task, TaskType


class TestTaskAdminNextRun(TestCase):
    def setUp(self) -> None:
        self.admin = TaskAdmin(Task, admin.site)

    def test_next_run_returns_not_scheduled_when_scheduled_time_is_none(self) -> None:
        task = Task(
            name="unscheduled task",
            task_type=TaskType.ONCE.value,
            queue="default",
            callable="scheduler.tests.jobs.test_job",
            enabled=False,
            scheduled_time=None,
        )

        # Regression for #363: comparison against timezone.now() must not run
        # when scheduled_time is None, otherwise a TypeError is raised when the
        # admin list view renders the column.
        assert self.admin.next_run(task) == "Not scheduled"

    def test_next_run_returns_datetime_for_future_scheduled_time(self) -> None:
        future = timezone.now() + timedelta(days=1)
        task = Task(
            name="future task",
            task_type=TaskType.ONCE.value,
            queue="default",
            callable="scheduler.tests.jobs.test_job",
            enabled=True,
            scheduled_time=future,
        )

        result = self.admin.next_run(task)

        assert not isinstance(result, str)
        assert result == future or result == timezone.make_aware(future, timezone.get_current_timezone())
