from django import forms
from django.urls import reverse

from scheduler.admin.task_admin import JobMethodsDatalistWidget, get_job_executions_for_task
from scheduler.decorators import JOB_METHODS_LIST
from scheduler.models import TaskType
from scheduler.tests import conf  # noqa
from scheduler.tests.testtools import SchedulerBaseCase, task_factory

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


class TestTaskAdminQuerySetAndIsScheduled(SchedulerBaseCase):
    def test_task_admin_changelist_prefetches_args(self):
        self.client.login(username="admin", password="admin")
        task_factory(TaskType.ONCE, queue="default")
        task_factory(TaskType.ONCE, queue="default")

        res = self.client.get(reverse("admin:scheduler_task_changelist"))
        self.assertEqual(res.status_code, 200)

    def test_is_scheduled_is_read_only_and_does_not_mutate_db(self):
        task = task_factory(TaskType.ONCE, queue="default")
        task.rqueue.connection.flushall()
        # With redis flushed, is_scheduled returns False
        with self.assertNumQueries(0):
            self.assertFalse(task.is_scheduled())
