import threading
from unittest.mock import MagicMock, patch

from django.urls import reverse

from scheduler import settings
from scheduler.helpers.callback import Callback
from scheduler.helpers.queues import Queue, get_queue
from scheduler.helpers.utils import current_timestamp
from scheduler.redis_models import (
    JobModel,
    JobNamesRegistry,
    KvLock,
    QueuedJobRegistry,
    Result,
    ResultType,
    SchedulerLock,
)
from scheduler.redis_models.lock import QueueLock
from scheduler.tests import conf  # noqa
from scheduler.tests.jobs import failing_job, test_args_kwargs, test_job
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.worker import create_worker


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


class TestQueuedJobRegistry(SchedulerBaseCase):
    def test_enqueue_job_at_front__dequeued_first(self):
        # arrange
        queue = get_queue("default")
        normal_job = queue.create_and_enqueue_job(test_job)
        # act
        at_front_job = queue.create_and_enqueue_job(test_job, at_front=True)
        # assert
        scores = dict(queue.queued_job_registry.all_with_timestamps(queue.connection))
        self.assertLess(scores[at_front_job.name], scores[normal_job.name])
        _, job_name = JobNamesRegistry.pop(queue.connection, [queue.queued_job_registry], None)
        self.assertEqual(at_front_job.name, job_name)

    def test_empty__more_than_1001_jobs__registry_emptied(self):
        # arrange
        queue = get_queue("default")
        registry = queue.queued_job_registry
        connection = queue.connection
        connection.zadd(registry.key, {f"job-{i}": float(i) for i in range(1005)})
        self.assertEqual(1005, registry.count(connection))
        # act
        registry.empty(connection)
        # assert
        self.assertEqual(0, registry.count(connection))


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

    def test_abandoned_job_without_failure_callback__moved_to_failed_registry(self):
        # A job with no failure callback is still abandoned, and the sweep at the end of
        # clean_registries drops it from the active registry either way -- so it has to be recorded
        # in the failed registry, not silently forgotten with status=STARTED.
        # arrange
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job, timeout=60)
        queue.active_job_registry.add(queue.connection, job.name, current_timestamp() - 3600)
        # act
        queue.clean_registries()
        # assert
        self.assertTrue(queue.failed_job_registry.exists(queue.connection, job.name))
        self.assertFalse(queue.active_job_registry.exists(queue.connection, job.name))

    def test_abandoned_job_is_handled_after_one_timeout_not_two(self):
        # The active registry entry is scored `started_at + timeout`, so an entry that
        # `get_job_names_before` returns is already expired. Re-testing `job_score + timeout` held a
        # job back until `started_at + 2 * timeout`; here the job is one timeout past its expiry,
        # which is short of that doubled threshold.
        # arrange
        queue = get_queue("default")
        timeout = 60
        job = queue.create_and_enqueue_job(test_job, timeout=timeout)
        started_at = current_timestamp() - timeout - 1
        queue.active_job_registry.add(queue.connection, job.name, started_at + timeout)
        # act
        queue.clean_registries()
        # assert
        self.assertTrue(queue.failed_job_registry.exists(queue.connection, job.name))

    def test_abandoned_job_with_failure_callback__moved_to_failed_registry(self):
        # arrange
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(failing_job, timeout=60, on_failure=Callback(test_args_kwargs))
        queue.active_job_registry.add(queue.connection, job.name, current_timestamp() - 3600)
        # act
        queue.clean_registries()
        # assert
        self.assertTrue(queue.failed_job_registry.exists(queue.connection, job.name))


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


class TestKvLock(SchedulerBaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.connection = get_queue("default").connection
        self.holder = KvLock("test-queue")
        self.assertTrue(self.holder.acquire(val="worker-1", connection=self.connection, expire=60))

    def test_acquire__lock_held__fails_without_waiting(self):
        self.assertFalse(KvLock("test-queue").acquire(val="worker-2", connection=self.connection, expire=60))

        self.assertEqual(b"worker-1", self.holder.value(self.connection))

    def test_release__held__releases_it(self):
        self.assertTrue(self.holder.release())

        self.assertIsNone(self.holder.value(self.connection))
        self.assertFalse(self.holder.acquired)

    def test_release__from_another_thread__releases_it(self):
        # The scheduler takes its locks in the worker's thread and releases them from its own.
        released = []
        thread = threading.Thread(target=lambda: released.append(self.holder.release()))
        thread.start()
        thread.join()

        self.assertEqual([True], released)
        self.assertIsNone(self.holder.value(self.connection))

    def test_release__held_by_another__leaves_it(self):
        other = KvLock("test-queue")
        other.acquire(val="worker-2", connection=self.connection, expire=60)

        self.assertFalse(other.release())

        self.assertEqual(b"worker-1", self.holder.value(self.connection))

    def test_release__never_acquired__returns_false(self):
        self.assertFalse(KvLock("test-queue").release())

        self.assertEqual(b"worker-1", self.holder.value(self.connection))

    def test_release__taken_over_after_expiring__leaves_the_new_holders_lock(self):
        self.connection.delete(self.holder._locking_key)  # the lock expired
        self.assertTrue(KvLock("test-queue").acquire(val="worker-2", connection=self.connection, expire=60))

        self.assertFalse(self.holder.release())

        self.assertEqual(b"worker-2", self.holder.value(self.connection))

    def test_expire__held__resets_the_ttl(self):
        self.assertTrue(self.holder.expire(120))

        self.assertGreater(self.connection.ttl(self.holder._locking_key), 60)

    def test_expire__held_by_another__leaves_the_ttl(self):
        other = KvLock("test-queue")
        other.acquire(val="worker-2", connection=self.connection, expire=60)

        self.assertFalse(other.expire(120))

        self.assertLessEqual(self.connection.ttl(self.holder._locking_key), 60)

    def test_scheduler_lock__value_is_the_token(self):
        lock = SchedulerLock("default")
        self.assertTrue(lock.acquire(val=12345, connection=self.connection, expire=60))

        self.assertEqual(b"12345", lock.value(self.connection))
        self.assertTrue(lock.release())
        self.assertIsNone(lock.value(self.connection))


class TestWorkerCleanRegistriesLock(SchedulerBaseCase):
    def test_clean_registries__releases_the_queue_lock(self):
        queue = get_queue("default")

        create_worker("default", burst=True).clean_registries()

        self.assertIsNone(QueueLock("default").value(queue.connection))

    def test_clean_registries__lock_held_by_another_worker__skips_the_queue_and_leaves_the_lock(self):
        queue = get_queue("default")
        QueueLock("default").acquire(val="other-worker", connection=queue.connection, expire=60)

        with patch.object(Queue, "clean_registries") as clean:
            create_worker("default", burst=True).clean_registries()

        clean.assert_not_called()
        self.assertEqual(b"other-worker", QueueLock("default").value(queue.connection))


class TestJobModelTaskIndexing(SchedulerBaseCase):
    def test_prune_task_index__drops_only_expired_jobs(self):
        queue = get_queue("default")
        conn = queue.connection
        expired = queue.create_and_enqueue_job(test_job, scheduled_task_id=999)
        alive = queue.create_and_enqueue_job(test_job, scheduled_task_id=999)
        conn.delete(JobModel.key_for(expired.name))  # the job's hash expired

        JobModel.prune_task_index(999, conn)

        self.assertEqual({alive.name.encode()}, conn.smembers(JobModel._task_key_template.format(999)))

    def test_job_model_indexes_task_jobs_on_save_and_cleans_on_delete(self):
        queue = get_queue("default")
        conn = queue.connection
        task_id = 999
        job = queue.create_and_enqueue_job(test_job, scheduled_task_id=task_id)

        # Check that job is in task jobs set
        task_jobs = JobModel.get_jobs_for_task(task_id, conn)
        self.assertEqual(len(task_jobs), 1)
        self.assertEqual(task_jobs[0].name, job.name)

        # Act: delete job
        job.delete(conn)

        # Assert: task jobs set is now empty
        task_jobs_after = JobModel.get_jobs_for_task(task_id, conn)
        self.assertEqual(len(task_jobs_after), 0)


class TestQueueBatchDelete(SchedulerBaseCase):
    def test_queue_delete_jobs__batch_deletes_jobs(self):
        queue = get_queue("default")
        job1 = queue.create_and_enqueue_job(test_job, job_info_ttl=0)
        job2 = queue.create_and_enqueue_job(test_job, job_info_ttl=0)

        self.assertTrue(JobModel.exists(job1.name, connection=queue.connection))
        self.assertTrue(JobModel.exists(job2.name, connection=queue.connection))

        queue.delete_jobs([job1.name, job2.name])

        self.assertFalse(JobModel.exists(job1.name, connection=queue.connection))
        self.assertFalse(JobModel.exists(job2.name, connection=queue.connection))
        self.assertNotIn(job1.name, queue.queued_job_registry.all(queue.connection))
        self.assertNotIn(job2.name, queue.queued_job_registry.all(queue.connection))


class TestQueuedJobRegistryCompact(SchedulerBaseCase):
    def test_compact__removes_only_missing_jobs(self):
        queue = get_queue("default")
        gone = queue.create_and_enqueue_job(test_job)
        kept = queue.create_and_enqueue_job(test_job)
        queue.connection.delete(JobModel.key_for(gone.name))

        queue.queued_job_registry.compact(queue.connection)

        self.assertEqual([kept.name], queue.queued_job_registry.all(queue.connection))

    def test_dequeue_any__skips_missing_jobs_without_compacting(self):
        queue = get_queue("default")
        gone = queue.create_and_enqueue_job(test_job)
        kept = queue.create_and_enqueue_job(test_job)
        queue.connection.delete(JobModel.key_for(gone.name))

        with patch.object(QueuedJobRegistry, "compact", side_effect=AssertionError("compacted on dequeue")):
            job, dequeued_from = Queue.dequeue_any([queue], None, queue.connection)

        self.assertEqual(kept.name, job.name)
        self.assertEqual(queue.name, dequeued_from.name)


class TestQueueGetAllJobNames(SchedulerBaseCase):
    def test_get_all_job_names__checks_existence_in_one_round_trip(self):
        queue = get_queue("default")
        gone = queue.create_and_enqueue_job(test_job)
        kept = queue.create_and_enqueue_job(test_job)
        queue.connection.delete(JobModel.key_for(gone.name))

        with patch.object(JobModel, "exists", side_effect=AssertionError("one round trip per job")):
            self.assertEqual([kept.name], queue.get_all_job_names())


class TestQueueRegistryCounts(SchedulerBaseCase):
    def test_registry_counts__cleans_up_and_counts_in_one_round_trip(self):
        queue = get_queue("default")
        queue.create_and_enqueue_job(test_job)
        queue.failed_job_registry.add(queue.connection, "failed-job", current_timestamp() + 100)
        queue.finished_job_registry.add(queue.connection, "expired-job", current_timestamp() - 1)

        with patch.object(queue.connection, "pipeline", wraps=queue.connection.pipeline) as pipeline:
            counts = queue.registry_counts()

        pipeline.assert_called_once()
        self.assertEqual({"queued": 1, "failed": 1, "finished": 0, "active": 0, "scheduled": 0, "canceled": 0}, counts)
        self.assertEqual(2, queue.count)


class TestJobNamesRegistryGetFirst(SchedulerBaseCase):
    def test_get_first__connection_returning_str__returns_the_name(self):
        connection = MagicMock()
        connection.zrange.return_value = ["job-1"]

        self.assertEqual("job-1", QueuedJobRegistry("default").get_first(connection))
