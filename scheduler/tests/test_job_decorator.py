import time

from django.test import TestCase

from scheduler import job, settings
from . import test_settings  # noqa
from ..queues import get_queue, QueueNotFoundError


@job
def test_job():
    time.sleep(1)
    return 1 + 1


@job('django_rq_scheduler_test')
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


class JobDecoratorTest(TestCase):
    def setUp(self) -> None:
        get_queue('default').connection.flushall()

    def test_job_decorator_no_params(self):
        test_job.delay()
        config = settings.SCHEDULER_CONFIG
        self._assert_job_with_func_and_props(
            'default', test_job, config.get('DEFAULT_RESULT_TTL'), config.get('DEFAULT_TIMEOUT'))

    def test_job_decorator_timeout(self):
        test_job_timeout.delay()
        config = settings.SCHEDULER_CONFIG
        self._assert_job_with_func_and_props(
            'default', test_job_timeout, config.get('DEFAULT_RESULT_TTL'), 1)

    def test_job_decorator_result_ttl(self):
        test_job_result_ttl.delay()
        config = settings.SCHEDULER_CONFIG
        self._assert_job_with_func_and_props(
            'default', test_job_result_ttl, 1, config.get('DEFAULT_TIMEOUT'))

    def test_job_decorator_different_queue(self):
        test_job_diff_queue.delay()
        config = settings.SCHEDULER_CONFIG
        self._assert_job_with_func_and_props(
            'django_rq_scheduler_test',
            test_job_diff_queue,
            config.get('DEFAULT_RESULT_TTL'),
            config.get('DEFAULT_TIMEOUT'))

    def _assert_job_with_func_and_props(
            self, queue_name,
            expected_func,
            expected_result_ttl,
            expected_timeout):
        queue = get_queue(queue_name)
        jobs = queue.get_jobs()
        self.assertEqual(1, len(jobs))

        j = jobs[0]
        self.assertEqual(j.func, expected_func)
        self.assertEqual(j.result_ttl, expected_result_ttl)
        self.assertEqual(j.timeout, expected_timeout)

    def test_job_decorator_bad_queue(self):
        with self.assertRaises(QueueNotFoundError):
            @job('bad-queue')
            def test_job_bad_queue():
                time.sleep(1)
                return 1 + 1
