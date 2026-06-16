from django.core.paginator import Paginator
from django.template.loader import render_to_string
from django.urls import reverse

from scheduler.helpers.queues import get_queue
from scheduler.redis_models import JobModel
from scheduler.templatetags.scheduler_tags import job_result, latest_result
from scheduler.worker import create_worker
from scheduler.tests import conf  # noqa
from scheduler.tests.test_views.base import BaseTestCase

_QUEUE = "django_tasks_scheduler_test"
_JOBS_LIST_PARTIALS = (
    "admin/scheduler/jobs-list.partial.html",
    "admin/scheduler/jobs-list-with-tasks.partial.html",
)


def job_with_distinctive_return_value():
    return "distinctive-return-value-42"


class TestViewWorkers(BaseTestCase):
    def test_workers_home(self):
        res = self.client.get(reverse("workers_home"))
        prev_workers = res.context["workers"]
        worker1 = create_worker("django_tasks_scheduler_test")
        worker1.worker_start()
        worker2 = create_worker("test3")
        worker2.worker_start()

        res = self.client.get(reverse("workers_home"))
        self.assertEqual(res.context["workers"], prev_workers + [worker1._model, worker2._model])

    def test_jobs_list_partials__render_return_value(self):
        """The job execution list exposes the callable's return value (issue #336)."""
        queue = get_queue(_QUEUE)
        job = queue.create_and_enqueue_job(job_with_distinctive_return_value)
        worker = create_worker(_QUEUE, burst=True)
        worker.work()
        executed = JobModel.get(name=job.name, connection=queue.connection)
        page = Paginator([executed], 20).get_page(1)

        for template_name in _JOBS_LIST_PARTIALS:
            with self.subTest(template=template_name):
                html = render_to_string(template_name, {"executions": page})
                self.assertIn("Return value", html)  # new column header
                self.assertIn("Successful", html)  # result type column
                self.assertIn("distinctive-return-value-42", html)  # the callable's return value


class TestJobResultFilters(BaseTestCase):
    def test_latest_result__returns_result_with_return_value(self):
        queue = get_queue(_QUEUE)
        job = queue.create_and_enqueue_job(job_with_distinctive_return_value)
        worker = create_worker(_QUEUE, burst=True)
        worker.work()

        result = latest_result(job)
        self.assertIsNotNone(result)
        self.assertEqual("distinctive-return-value-42", result.return_value)
        self.assertEqual("Successful", job_result(job))

    def test_latest_result__none_before_execution(self):
        queue = get_queue(_QUEUE)
        job = queue.create_and_enqueue_job(job_with_distinctive_return_value)

        self.assertIsNone(latest_result(job))
        self.assertIsNone(job_result(job))
