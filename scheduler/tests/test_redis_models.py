from django.urls import reverse

from scheduler import settings
from scheduler.helpers.callback import Callback
from scheduler.helpers.queues import get_queue
from scheduler.helpers.utils import current_timestamp
from scheduler.redis_models import Result, ResultType
from scheduler.tests.jobs import failing_job, test_job
from scheduler.tests.testtools import SchedulerBaseCase


class TestWorkerAdmin(SchedulerBaseCase):
    def test_admin_list_view(self):
        # arrange
        self.client.login(username="admin", password="admin")
        model = "worker"
        url = reverse(f"admin:scheduler_{model}_changelist")

        # act
        res = self.client.get(url)
        # assert
        self.assertEqual(200, res.status_code)


class TestResult(SchedulerBaseCase):
    def _create_result(self, job_name: str, ttl: int) -> str:
        """Creates a successful result for `job_name`, returning the key of the job results stream."""
        Result.create(
            get_queue("default").connection,
            job_name=job_name,
            worker_name="worker-name",
            _type=ResultType.SUCCESSFUL,
            ttl=ttl,
            return_value=1,
        )
        return Result._children_key_template.format(job_name)

    def test_result_positive_ttl__expires_stream(self):
        # arrange
        queue = get_queue("default")
        # act
        key = self._create_result("job-positive-ttl", ttl=100)
        # assert
        self.assertEqual(100, queue.connection.ttl(key))
        self.assertIsNotNone(Result.fetch_latest(queue.connection, "job-positive-ttl"))

    def test_result_zero_ttl__deletes_stream(self):
        # arrange
        queue = get_queue("default")
        # act
        key = self._create_result("job-zero-ttl", ttl=0)
        # assert
        self.assertEqual(0, queue.connection.exists(key))
        self.assertIsNone(Result.fetch_latest(queue.connection, "job-zero-ttl"))

    def test_result_negative_ttl__keeps_stream_indefinitely(self):
        # arrange
        queue = get_queue("default")
        # act
        key = self._create_result("job-negative-ttl", ttl=-1)
        # assert
        self.assertEqual(1, queue.connection.exists(key))
        self.assertEqual(-1, queue.connection.ttl(key))
        self.assertIsNotNone(Result.fetch_latest(queue.connection, "job-negative-ttl"))

    def test_result_negative_ttl__removes_existing_expiry(self):
        # arrange
        queue = get_queue("default")
        self._create_result("job-mixed-ttl", ttl=100)
        # act
        key = self._create_result("job-mixed-ttl", ttl=-1)
        # assert
        self.assertEqual(-1, queue.connection.ttl(key))

    def test_job_handle_success__expires_result_stream(self):
        # arrange
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job, result_ttl=100)
        # act
        queue.run_sync(job)
        # assert
        self.assertEqual(100, queue.connection.ttl(Result._children_key_template.format(job.name)))

    def test_job_handle_success_without_result_ttl__expires_result_stream(self):
        # arrange
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job)
        # act
        queue.run_sync(job)
        # assert
        self.assertEqual(
            settings.SCHEDULER_CONFIG.DEFAULT_SUCCESS_TTL,
            queue.connection.ttl(Result._children_key_template.format(job.name)),
        )

    def test_job_handle_failure__expires_result_stream(self):
        # arrange
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(failing_job)
        # act
        queue.run_sync(job)
        # assert
        self.assertEqual(
            settings.SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL,
            queue.connection.ttl(Result._children_key_template.format(job.name)),
        )


class TestJobModelHasFailureCallback(SchedulerBaseCase):
    def test_job_without_failure_callback__has_failure_callback_is_false(self):
        # arrange
        queue = get_queue("default")
        # act
        job = queue.create_and_enqueue_job(test_job)
        # assert
        self.assertIs(False, job.has_failure_callback)

    def test_job_with_failure_callback__has_failure_callback_is_true(self):
        # arrange
        queue = get_queue("default")
        # act
        job = queue.create_and_enqueue_job(failing_job, on_failure=Callback(test_job))
        # assert
        self.assertIs(True, job.has_failure_callback)


class TestQueueCleanRegistries(SchedulerBaseCase):
    def test_no_abandoned_jobs__expired_registry_entries_are_swept(self):
        # arrange
        queue = get_queue("default")
        registry = queue.finished_job_registry
        registry.add(queue.connection, "expired-job", current_timestamp() - 100)
        self.assertTrue(registry.exists(queue.connection, "expired-job"))
        # act
        queue.clean_registries()
        # assert
        self.assertFalse(registry.exists(queue.connection, "expired-job"))

    def test_abandoned_job_without_failure_callback__not_moved_to_failed_registry(self):
        # arrange
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job, timeout=60)
        queue.active_job_registry.add(queue.connection, job.name, current_timestamp() - 3600)
        # act
        queue.clean_registries()
        # assert
        self.assertFalse(queue.failed_job_registry.exists(queue.connection, job.name))


class TestQueueAdmin(SchedulerBaseCase):
    def test_admin_list_view(self):
        # arrange
        self.client.login(username="admin", password="admin")
        model = "queue"
        url = reverse(f"admin:scheduler_{model}_changelist")

        # act
        res = self.client.get(url)
        # assert
        self.assertEqual(200, res.status_code)
