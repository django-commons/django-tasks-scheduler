from django import forms
from django.urls import reverse

from scheduler.admin.task_admin import JobMethodsDatalistWidget
from scheduler.decorators import JOB_METHODS_LIST
from scheduler.tests import conf  # noqa
from scheduler.tests.testtools import SchedulerBaseCase

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
