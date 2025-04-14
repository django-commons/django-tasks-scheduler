import time

from django.test import TestCase

from scheduler import settings
from scheduler.helpers.queues import get_queue
from . import test_settings  # noqa
from ..decorators import JOB_METHODS_LIST, job
from ..redis_models import JobStatus
from ..redis_models.job import JobModel
from ..worker import create_worker


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
def long_running_func(x):
    x.run()


class JobDecoratorTest(TestCase):
    def setUp(self) -> None:
        get_queue("default").connection.flushall()

    def test_all_job_methods_registered(self):
        self.assertEqual(5, len(JOB_METHODS_LIST))

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
        jobs = JobModel.get_many(queue.queued_job_registry.all(), queue.connection)
        self.assertEqual(1, len(jobs))

        j = jobs[0]
        self.assertEqual(j.func, expected_func)
        self.assertEqual(j.success_ttl, expected_result_ttl)
        self.assertEqual(j.timeout, expected_timeout)

    def test_job_decorator_bad_queue(self):
        with self.assertRaises(settings.QueueNotFoundError):

            @job("bad-queue")
            def test_job_bad_queue():
                time.sleep(1)
                return 1 + 1

    def test_job_decorator_delay_with_param(self):
        queue_name = "default"
        long_running_func.delay(MyClass())

        worker = create_worker(queue_name, burst=True)
        worker.work()

        jobs_list = worker.queues[0].get_all_jobs()
        self.assertEqual(1, len(jobs_list))
        job = jobs_list[0]
        self.assertEqual(job.func, long_running_func)
        self.assertEqual(job.kwargs, {})
        self.assertEqual(job.status, JobStatus.FINISHED)
        self.assertEqual(job.args, (MyClass(),))
