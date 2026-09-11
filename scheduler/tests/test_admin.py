from datetime import timedelta
from typing import Any
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
from scheduler.redis_models import Result
from scheduler.tests import conf  # noqa
from scheduler.tests.testtools import SchedulerBaseCase, task_factory, taskarg_factory
from scheduler.worker import create_worker

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


class TestTaskAdminBulkActions(SchedulerBaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.client.login(username="admin", password="admin")
        self.url = reverse("admin:scheduler_task_changelist")

    def _post(self, url: str, data: dict[str, Any]) -> list[str]:
        with CaptureQueriesContext(connection) as queries:
            res = self.client.post(url, data=data)
        self.assertEqual(302, res.status_code)
        return [q["sql"] for q in queries.captured_queries]

    def _assert_nothing_scheduled(self) -> None:
        queue = Task(queue="default").rqueue
        self.assertEqual([], queue.scheduled_job_registry.all(queue.connection))

    def test_disable_selected__query_count_does_not_grow_with_tasks(self):
        one = self._post(self.url, {"action": "disable_selected", "_selected_action": [task_factory(TaskType.CRON).id]})
        ids = [task_factory(TaskType.CRON).id for _ in range(3)]

        three = self._post(self.url, {"action": "disable_selected", "_selected_action": ids})

        self.assertEqual(len(one), len(three))

    def test_disable_selected__unschedules_in_one_broker_call_per_queue(self):
        tasks = [task_factory(TaskType.CRON) for _ in range(3)]

        with patch.object(Queue, "delete_job", side_effect=AssertionError("one broker call per task")):
            self._post(self.url, {"action": "disable_selected", "_selected_action": [task.id for task in tasks]})

        self.assertEqual({(False, None)}, set(Task.objects.values_list("enabled", "job_name")))
        self._assert_nothing_scheduled()

    def test_delete_selected__unschedules_in_one_broker_call_per_queue(self):
        tasks = [task_factory(TaskType.CRON) for _ in range(3)]
        data = {"action": "delete_selected", "_selected_action": [task.id for task in tasks], "post": "yes"}

        with patch.object(Queue, "delete_job", side_effect=AssertionError("one broker call per task")):
            self._post(self.url, data)

        self.assertFalse(Task.objects.exists())
        self._assert_nothing_scheduled()

    def test_delete_model__does_not_update_the_row_first(self):
        task = task_factory(TaskType.CRON)

        sql = self._post(reverse("admin:scheduler_task_delete", args=[task.id]), {"post": "yes"})

        self.assertEqual([], [statement for statement in sql if statement.startswith('UPDATE "scheduler_task"')])
        self.assertFalse(Task.objects.exists())
        self._assert_nothing_scheduled()


class TestTaskAdminChangeView(SchedulerBaseCase):
    def test_execution_results_are_fetched_in_one_round_trip(self):
        self.client.login(username="admin", password="admin")
        task = task_factory(TaskType.ONCE)
        for _ in range(3):
            task.enqueue_to_run()
        create_worker(task.queue, burst=True, fork_job_execution=False).work()

        with patch.object(Result, "fetch_latest", side_effect=AssertionError("fetched a result per row")):
            res = self.client.get(reverse("admin:scheduler_task_change", args=[task.id]))

        self.assertEqual(200, res.status_code)
        self.assertEqual(3, len(res.context["latest_results"]))


class TestTaskIsScheduled(SchedulerBaseCase):
    def test_is_scheduled_is_read_only_and_does_not_mutate_db(self):
        task = task_factory(TaskType.ONCE, queue="default")
        task.rqueue.connection.flushall()
        # With redis flushed, is_scheduled returns False
        with self.assertNumQueries(0):
            self.assertFalse(task.is_scheduled())
