from datetime import timedelta
from unittest.mock import patch

from django import forms
from django.db import connection
from django.http import HttpResponse
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from scheduler.admin.task_admin import JobMethodsDatalistWidget, get_job_executions_for_task
from scheduler.decorators import JOB_METHODS_LIST
from scheduler.helpers.queues import Queue
from scheduler.models import Task, TaskArg, TaskType
from scheduler.tests import conf  # noqa
from scheduler.tests.testtools import SchedulerBaseCase, task_factory, taskarg_factory

_METHOD = "scheduler.tests.test_admin.sample_registered_job"


class TestJobMethodsDatalistWidget(SchedulerBaseCase):
    def setUp(self) -> None:
        super().setUp()
        # Inject a known @job-registered callable for the duration of the test (and clean it up afterwards) so the
        # global JOB_METHODS_LIST - and the count asserted by test_all_job_methods_registered - is left untouched.
        JOB_METHODS_LIST.append(_METHOD)
        self.addCleanup(JOB_METHODS_LIST.remove, _METHOD)

    def test_widget_is_free_text_input(self):
        # A datalist suggests options but does not restrict input (callable accepts any importable path).
        self.assertIsInstance(JobMethodsDatalistWidget(), forms.TextInput)
        self.assertNotIsInstance(JobMethodsDatalistWidget(), forms.Select)

    def test_render_includes_registered_jobs_as_datalist_options(self):
        html = JobMethodsDatalistWidget().render("callable", None)
        self.assertIn('list="id_callable_job_methods"', html)
        self.assertIn('<datalist id="id_callable_job_methods">', html)
        self.assertIn(f'<option value="{_METHOD}">', html)


class TestTaskAdminCallableAutocomplete(SchedulerBaseCase):
    def setUp(self) -> None:
        super().setUp()
        JOB_METHODS_LIST.append(_METHOD)
        self.addCleanup(JOB_METHODS_LIST.remove, _METHOD)
        self.client.login(username="admin", password="admin")

    def test_add_form_offers_registered_jobs_as_datalist_options(self):
        res = self.client.get(reverse("admin:scheduler_task_add"))
        self.assertEqual(200, res.status_code)
        self.assertContains(res, 'list="id_callable_job_methods"')
        self.assertContains(res, '<datalist id="id_callable_job_methods">')
        self.assertContains(res, f'<option value="{_METHOD}">')


class TestTaskAdminJobExecutions(SchedulerBaseCase):
    def test_get_job_executions_for_task__returns_only_matching_jobs(self):
        task1 = task_factory(TaskType.ONCE, queue="default")
        task2 = task_factory(TaskType.ONCE, queue="default")

        task1_jobs = get_job_executions_for_task(task1.queue, task1)
        task2_jobs = get_job_executions_for_task(task2.queue, task2)

        self.assertEqual(len(task1_jobs), 1)
        self.assertEqual(task1_jobs[0].name, task1.job_name)
        self.assertEqual(len(task2_jobs), 1)
        self.assertEqual(task2_jobs[0].name, task2.job_name)


class TestTaskAdminChangelist(SchedulerBaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.client.login(username="admin", password="admin")
        self.url = reverse("admin:scheduler_task_changelist")

    def _get_changelist(self) -> tuple[HttpResponse, list[dict[str, str]]]:
        with CaptureQueriesContext(connection) as queries:
            res = self.client.get(self.url)
        self.assertEqual(200, res.status_code)
        return res, queries.captured_queries

    def _task_with_arg(self) -> Task:
        task = task_factory(TaskType.ONCE)
        taskarg_factory(TaskArg, val="one", content_object=task)
        return task

    def test_query_count_does_not_grow_with_rows(self):
        self._task_with_arg()
        _, one_row = self._get_changelist()
        for _ in range(3):
            self._task_with_arg()

        _, four_rows = self._get_changelist()

        self.assertEqual(len(one_row), len(four_rows))

    def test_checks_whether_tasks_are_scheduled_in_one_broker_round_trip(self):
        self._task_with_arg()
        unscheduled = self._task_with_arg()
        unscheduled.rqueue.delete_job(unscheduled.job_name)

        with (
            patch.object(Task, "is_scheduled", side_effect=AssertionError("checked the broker once per row")),
            patch.object(Queue, "pending_job_names", autospec=True, side_effect=Queue.pending_job_names) as pending,
        ):
            res, _ = self._get_changelist()

        pending.assert_called_once()
        self.assertContains(res, 'alt="False"', count=1)  # the only False boolean on the page is `unscheduled`

    def test_task_past_its_scheduled_time__rendering_does_not_write(self):
        task = task_factory(TaskType.CRON)
        Task.objects.filter(id=task.id).update(scheduled_time=timezone.now() - timedelta(minutes=5))

        _, queries = self._get_changelist()

        self.assertEqual([], [q["sql"] for q in queries if q["sql"].startswith(("INSERT", "UPDATE", "DELETE"))])


class TestTaskIsScheduled(SchedulerBaseCase):
    def test_is_scheduled_is_read_only_and_does_not_mutate_db(self):
        task = task_factory(TaskType.ONCE, queue="default")
        task.rqueue.connection.flushall()
        # With redis flushed, is_scheduled returns False
        with self.assertNumQueries(0):
            self.assertFalse(task.is_scheduled())
