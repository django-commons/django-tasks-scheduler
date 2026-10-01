from datetime import timedelta

from scheduler.helpers.queues import get_queue
from scheduler.helpers.utils import utcnow
from scheduler.redis_models import JobModel
from scheduler.templatetags.scheduler_tags import job_runtime
from scheduler.tests.jobs import test_job
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.worker import create_worker

# Longer than a second, which is the whole point: `timedelta.microseconds` is the sub-second component,
# so anything at or above a second used to be recorded as roughly nothing.
RAN_FOR_SECONDS = 90


class TestWorkerTotalWorkingTime(SchedulerBaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.queue = get_queue("default")

    def _job_started_long_ago(self) -> JobModel:
        """A job that started `RAN_FOR_SECONDS` ago.

        `ended_at` is deliberately not set here: `after_execution()` stamps it with the current time
        while the outcome is being handled, so the runtime the worker accounts for is measured from
        `started_at`.
        """
        job = self.queue.create_and_enqueue_job(test_job, when=None)
        job.started_at = utcnow() - timedelta(seconds=RAN_FOR_SECONDS)
        job.ended_at = job.started_at
        return job

    def test_successful_job_accounts_for_its_whole_runtime(self) -> None:
        worker = create_worker("default", name="test-working-time-success")
        job = self._job_started_long_ago()

        worker.handle_job_success(job=job, return_value=None, queue=self.queue)

        # Before the fix this was the sub-second remainder only, always under 1000ms however long the job ran.
        self.assertGreater(worker._model.total_working_time_ms, 60_000)
        self.assertAlmostEqual(RAN_FOR_SECONDS * 1000, worker._model.total_working_time_ms, delta=5_000)

    def test_failed_job_accounts_for_its_whole_runtime(self) -> None:
        worker = create_worker("default", name="test-working-time-failure")
        job = self._job_started_long_ago()

        worker.handle_job_failure(job=job, queue=self.queue, exc_string="boom")

        self.assertGreater(worker._model.total_working_time_ms, 60_000)
        self.assertAlmostEqual(RAN_FOR_SECONDS * 1000, worker._model.total_working_time_ms, delta=5_000)

    def test_job_runtime_filter_reports_the_whole_runtime(self) -> None:
        """The job lists render this per row; it had the same sub-second bug and showed "5ms" for a 90s job."""
        job = self._job_started_long_ago()
        job.ended_at = job.started_at + timedelta(seconds=RAN_FOR_SECONDS)

        self.assertEqual(f"{RAN_FOR_SECONDS * 1000}ms", job_runtime(job))
