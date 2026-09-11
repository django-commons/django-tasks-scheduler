from unittest.mock import patch

from django.core.paginator import Paginator
from django.template.loader import render_to_string
from django.urls import reverse

from scheduler.helpers.queues import get_all_workers, get_queue
from scheduler.helpers.queues.getters import _queue_names_by_broker
from scheduler.redis_models import JobModel, Result, WorkerModel
from scheduler.settings import get_queue_names
from scheduler.templatetags.scheduler_tags import job_result, latest_result
from scheduler.tests import conf  # noqa
from scheduler.tests.test_views.base import BaseTestCase
from scheduler.worker import create_worker

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
        latest_results = Result.fetch_latest_many(queue.connection, [executed.name])

        for template_name in _JOBS_LIST_PARTIALS:
            with self.subTest(template=template_name):
                html = render_to_string(template_name, {"executions": page, "latest_results": latest_results})
                self.assertIn("Return value", html)  # new column header
                self.assertIn("Successful", html)  # result type column
                self.assertIn("distinctive-return-value-42", html)  # the callable's return value

    def test_worker_details__fetches_only_the_workers_own_jobs(self):
        queue = get_queue(_QUEUE)
        other_job = queue.create_and_enqueue_job(job_with_distinctive_return_value)
        create_worker(_QUEUE, name="other-worker", burst=True).work()
        own_job = queue.create_and_enqueue_job(job_with_distinctive_return_value)
        create_worker(_QUEUE, name="own-worker", burst=True).work()
        create_worker(_QUEUE, name="own-worker").worker_start()

        with patch.object(JobModel, "get_many", wraps=JobModel.get_many) as get_many:
            res = self.client.get(reverse("worker_details", args=["own-worker"]))

        self.assertEqual([own_job.name], [job.name for job in res.context["executions"]])
        fetched = {job_name for call in get_many.call_args_list for job_name in call.args[0]}
        self.assertNotIn(other_job.name, fetched)

    def test_worker_details__stopped_worker__404(self):
        create_worker(_QUEUE, name="stopped-worker", burst=True).work()

        res = self.client.get(reverse("worker_details", args=["stopped-worker"]))

        self.assertEqual(404, res.status_code)

    def test_worker_details__fetches_the_results_in_one_round_trip(self):
        queue = get_queue(_QUEUE)
        for _ in range(3):
            queue.create_and_enqueue_job(job_with_distinctive_return_value)
        create_worker(_QUEUE, name="details-worker", burst=True).work()
        create_worker(_QUEUE, name="details-worker").worker_start()

        with patch.object(Result, "fetch_latest", side_effect=AssertionError("fetched a result per row")):
            res = self.client.get(reverse("worker_details", args=["details-worker"]))

        self.assertEqual(3, len(res.context["latest_results"]))
        self.assertContains(res, "distinctive-return-value-42", count=3)


class TestWorkerListing(BaseTestCase):
    def test_worker_model_all__one_round_trip(self):
        for name in ("listed-1", "listed-2"):
            create_worker(_QUEUE, name=name).worker_start()

        with patch.object(WorkerModel, "get", side_effect=AssertionError("one round trip per worker")):
            workers = WorkerModel.all(get_queue(_QUEUE).connection)

        self.assertEqual({"listed-1", "listed-2"}, {worker.name for worker in workers})

    def test_get_all_workers__lists_each_broker_once(self):
        with patch.object(WorkerModel, "all", wraps=WorkerModel.all) as list_workers:
            get_all_workers()

        self.assertEqual(len(_queue_names_by_broker()), list_workers.call_count)
        self.assertLess(list_workers.call_count, len(get_queue_names()))


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
