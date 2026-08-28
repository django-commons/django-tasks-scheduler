"""The worker must kill a job execution process that hangs past its timeout.

The monitor loop runs in the parent, and the only copy of the job it has is the one it
dequeued before forking. `started_at` is recorded by the child, in its own copy and in the
broker, so the parent's copy never has one - the loop has nothing to measure the job's
working time from but the fork itself.
"""

from datetime import timedelta
from unittest import mock

from scheduler.helpers.queues import get_queue
from scheduler.helpers.timeouts import JobExecutionMonitorTimeoutException
from scheduler.helpers.utils import utcnow
from scheduler.redis_models import JobModel, JobStatus, WorkerModel
from scheduler.tests import conf  # noqa: F401
from scheduler.tests.jobs import long_job
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.worker import create_worker

JOB_EXECUTION_PROCESS_PID = 424242


class _Clock:
    """A stand-in for `utcnow` that only moves when a test moves it."""

    def __init__(self) -> None:
        self.now = utcnow()

    def __call__(self):
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class JobExecutionMonitoringTest(SchedulerBaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.queue = get_queue("default")
        self.worker = create_worker("default", name="test-monitoring", burst=True, with_scheduler=False)
        self.worker.worker_start()
        self.worker._model.job_execution_process_pid = JOB_EXECUTION_PROCESS_PID
        self.clock = _Clock()
        self.working_time_seen_by_the_broker = None

    def _enqueue_job(self, timeout: int) -> JobModel:
        job = self.queue.create_and_enqueue_job(long_job, timeout=timeout)
        self.assertIsNone(job.started_at, "test setup: the parent's copy should have no started_at")
        return job

    def _monitor(self, job: JobModel, hangs_for: list[float]):
        """Run the real monitor loop against a child that outlives one wait per entry in `hangs_for`.

        Only the two things the parent cannot do in a test are replaced: waiting on a real
        pid, and killing one. Each hanging wait burns its seconds on the clock and reports the
        child still running, which is what the death penalty around it does in production; a
        final wait lets the loop finish if the child was never killed.
        """

        def still_running(seconds: float):
            def wait():
                self.clock.advance(seconds)
                raise JobExecutionMonitorTimeoutException("job execution process still running")

            return wait

        def gone():
            # Read back what the loop published, before the loop resets it on the way out.
            model = WorkerModel.get(self.worker.name, connection=self.queue.connection)
            self.working_time_seen_by_the_broker = model.current_job_working_time
            return JOB_EXECUTION_PROCESS_PID, 0

        waits = iter([still_running(seconds) for seconds in hangs_for] + [gone])

        with (
            mock.patch.object(self.worker, "_kill_job_execution_process") as kill,
            mock.patch.object(self.worker, "_wait_for_job_execution_process", side_effect=lambda: next(waits)()),
            mock.patch("scheduler.worker.worker.utcnow", self.clock),
        ):
            self.worker.monitor_job_execution_process(job, self.queue)
        return kill

    def _reload(self, job: JobModel) -> JobModel:
        return JobModel.get(job.name, connection=self.queue.connection)

    def test_a_child_still_running_past_its_timeout_is_killed(self):
        job = self._enqueue_job(timeout=60)

        kill = self._monitor(job, hangs_for=[121])  # past timeout + 60

        kill.assert_called_once()

    def test_time_hung_accumulates_across_monitoring_intervals(self):
        # The real shape of it: the monitor wakes every job_monitoring_interval seconds, and
        # no single wait is longer than the timeout. The deadline is only reached by adding
        # them up, so the clock has to run from the fork and not from the current wait.
        job = self._enqueue_job(timeout=60)

        kill = self._monitor(job, hangs_for=[30, 30, 30, 31])  # 121 total, no single wait past 60

        kill.assert_called_once()
        self.assertEqual(121.0, self.working_time_seen_by_the_broker)

    def test_the_job_a_killed_child_was_running_is_marked_failed(self):
        job = self._enqueue_job(timeout=60)

        self._monitor(job, hangs_for=[121])

        self.assertEqual(JobStatus.FAILED, self._reload(job).status)

    def test_the_working_time_is_published_to_the_broker_while_the_child_runs(self):
        # The admin and the worker's own kill decision both read this. Left at 0, a hung
        # child looks idle and can never outstay its timeout.
        job = self._enqueue_job(timeout=600)

        self._monitor(job, hangs_for=[45])

        self.assertEqual(45.0, self.working_time_seen_by_the_broker)

    def test_a_child_within_its_timeout_is_left_alone(self):
        job = self._enqueue_job(timeout=600)

        kill = self._monitor(job, hangs_for=[121])

        kill.assert_not_called()

    def test_a_job_with_no_timeout_is_never_killed(self):
        job = self._enqueue_job(timeout=-1)

        kill = self._monitor(job, hangs_for=[60 * 60 * 24])

        kill.assert_not_called()
