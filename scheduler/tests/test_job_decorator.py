import threading
import time

from django.test import TestCase

from scheduler import settings
from scheduler.helpers.queues import get_queue
from . import conf  # noqa
from ..decorators import JOB_METHODS_LIST, job
from ..redis_models import JobStatus
from ..redis_models.job import JobModel
from ..worker import create_worker, get_current_job


@job()
def test_job():
    time.sleep(1)
    return 1 + 1


@job("django_tasks_scheduler_test")
def test_job_diff_queue():
    time.sleep(1)
    return 1 + 1


@job(timeout=1)
def test_job_timeout():
    time.sleep(1)
    return 1 + 1


@job(result_ttl=1)
def test_job_result_ttl():
    return 1 + 1


class MyClass:
    def run(self):
        print("Hello")

    def __eq__(self, other):
        if not isinstance(other, MyClass):
            return False
        return True


@job()
def func_with_param(x):
    x.run()


@job(timeout=1)
def long_running_func():
    time.sleep(1000)


@job()
def job_recording_current_job():
    """Within a job context, get_current_job() returns the running job; record its name in meta."""
    current = get_current_job()
    assert current is not None
    current.meta["seen_job_name"] = current.name


@job()
def job_asserting_no_current_job():
    """When the callable is invoked directly (no worker), get_current_job() returns None."""
    assert get_current_job() is None


class JobDecoratorTest(TestCase):
    def setUp(self) -> None:
        get_queue("default").connection.flushall()

    def test_all_job_methods_registered(self):
        self.assertEqual(9, len(JOB_METHODS_LIST))

    def test_get_current_job__within_job_context(self):
        enqueued = job_recording_current_job.delay()
        worker = create_worker("default", burst=True)
        worker.work()

        queue = get_queue("default")
        executed = JobModel.get(name=enqueued.name, connection=queue.connection)
        self.assertEqual(JobStatus.FINISHED, executed.status)
        self.assertEqual(enqueued.name, executed.meta["seen_job_name"])

    def test_get_current_job__outside_job_context_returns_none(self):
        self.assertIsNone(get_current_job())

    def test_get_current_job__direct_call_returns_none(self):
        # Calling the decorated function directly (no worker) runs outside a job context.
        job_asserting_no_current_job()

    def test_job_decorator_no_params(self):
        test_job.delay()
        self._assert_job_with_func_and_props(
            "default",
            test_job,
            settings.SCHEDULER_CONFIG.DEFAULT_SUCCESS_TTL,
            settings.SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT,
        )

    def test_job_decorator_timeout(self):
        test_job_timeout.delay()
        self._assert_job_with_func_and_props(
            "default",
            test_job_timeout,
            settings.SCHEDULER_CONFIG.DEFAULT_SUCCESS_TTL,
            1,
        )

    def test_job_decorator_result_ttl(self):
        test_job_result_ttl.delay()
        self._assert_job_with_func_and_props(
            "default",
            test_job_result_ttl,
            1,
            settings.SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT,
        )

    def test_job_decorator_different_queue(self):
        test_job_diff_queue.delay()
        self._assert_job_with_func_and_props(
            "django_tasks_scheduler_test",
            test_job_diff_queue,
            settings.SCHEDULER_CONFIG.DEFAULT_SUCCESS_TTL,
            settings.SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT,
        )

    def _assert_job_with_func_and_props(self, queue_name, expected_func, expected_result_ttl, expected_timeout):
        queue = get_queue(queue_name)
        jobs = JobModel.get_many(queue.queued_job_registry.all(queue.connection), queue.connection)
        self.assertEqual(1, len(jobs))

        j = jobs[0]
        self.assertEqual(j.func, expected_func)
        self.assertEqual(j.success_ttl, expected_result_ttl)
        self.assertEqual(j.timeout, expected_timeout)

    def test_job_decorator_bad_queue(self):
        with self.assertRaises(settings.QueueNotFoundError):

            @job("bad-queue")
            def test_job_bad_queue():
                return 1 + 1

    def test_job_decorator_delay_with_param(self):
        queue_name = "default"
        func_with_param.delay(MyClass())

        worker = create_worker(queue_name, burst=True)
        worker.work()

        jobs_list = worker.queues[0].get_all_jobs()
        self.assertEqual(1, len(jobs_list))
        job = jobs_list[0]
        self.assertEqual(job.func, func_with_param)
        self.assertEqual(job.kwargs, {})
        self.assertEqual(job.status, JobStatus.FINISHED)
        self.assertEqual(job.args, (MyClass(),))

    def test_job_decorator_delay_with_param_worker_thread(self):
        queue_name = "default"

        long_running_func.delay()

        worker = create_worker(queue_name, burst=True)
        t = threading.Thread(target=worker.work)
        t.start()
        t.join()

        jobs_list = get_queue(queue_name).get_all_jobs()
        self.assertEqual(1, len(jobs_list))
        j = jobs_list[0]
        self.assertEqual(j.func, long_running_func)
        self.assertEqual(j.kwargs, {})
        self.assertEqual(j.status, JobStatus.FAILED)
