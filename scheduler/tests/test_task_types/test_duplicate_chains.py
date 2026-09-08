"""Regression tests for #412: manual runs and stale saves must not start a second recurring chain."""

from scheduler.helpers.queues import get_queue
from scheduler.models import Task, TaskType
from scheduler.redis_models import JobModel
from scheduler.tests import conf  # noqa: F401
from scheduler.tests.testtools import SchedulerBaseCase, task_factory
from scheduler.worker import create_worker


def _run_pending_jobs() -> None:
    create_worker("default", fork_job_execution=False, burst=True).work()


class TestNoDuplicateChains(SchedulerBaseCase):
    def scheduled_job_names(self) -> set[str]:
        queue = get_queue()
        return set(queue.scheduled_job_registry.all(queue.connection))

    def enqueue_scheduled_job(self, job_name: str) -> None:
        """Enqueue a scheduled job the way the scheduler does once its time has come."""
        queue = get_queue()
        queue.enqueue_job(JobModel.get(job_name, connection=queue.connection))

    def test_enqueue_to_run_keeps_a_single_chain(self):
        task = task_factory(TaskType.CRON)
        successor = task.job_name

        task.enqueue_to_run()
        _run_pending_jobs()

        self.assertEqual({successor}, self.scheduled_job_names())
        task.refresh_from_db()
        self.assertEqual(successor, task.job_name)
        self.assertEqual(1, task.successful_runs)
        self.assertIsNotNone(task.last_successful_run)

    def test_repeated_enqueue_to_run_keeps_a_single_chain(self):
        task = task_factory(TaskType.CRON)
        successor = task.job_name

        for _ in range(3):
            task.refresh_from_db()
            task.enqueue_to_run()
            _run_pending_jobs()

        self.assertEqual({successor}, self.scheduled_job_names())
        task.refresh_from_db()
        self.assertEqual(3, task.successful_runs)

    def test_failing_enqueue_to_run_keeps_a_single_chain(self):
        task = task_factory(TaskType.CRON, callable_name="scheduler.tests.jobs.failing_job")
        successor = task.job_name

        task.enqueue_to_run()
        _run_pending_jobs()

        self.assertEqual({successor}, self.scheduled_job_names())
        task.refresh_from_db()
        self.assertEqual(successor, task.job_name)
        self.assertEqual(1, task.failed_runs)
        self.assertEqual(0, task.successful_runs)

    def test_scheduled_run_still_schedules_its_successor(self):
        task = task_factory(TaskType.CRON)
        first_run = task.job_name

        self.enqueue_scheduled_job(first_run)
        _run_pending_jobs()

        task.refresh_from_db()
        self.assertNotEqual(first_run, task.job_name)
        self.assertEqual({task.job_name}, self.scheduled_job_names())
        self.assertEqual(1, task.successful_runs)

    def test_manual_run_alongside_the_scheduled_run_keeps_a_single_chain(self):
        """Both jobs finish in the same worker pass; only the scheduled one owns the chain."""
        task = task_factory(TaskType.CRON)
        first_run = task.job_name

        task.enqueue_to_run()
        self.enqueue_scheduled_job(first_run)
        _run_pending_jobs()

        task.refresh_from_db()
        self.assertNotEqual(first_run, task.job_name)
        self.assertEqual({task.job_name}, self.scheduled_job_names())
        self.assertEqual(2, task.successful_runs)

    def test_stale_task_save_does_not_add_a_chain(self):
        task = task_factory(TaskType.CRON)
        stale = Task.objects.get(id=task.id)  # what the scheduler loop materializes

        self.enqueue_scheduled_job(stale.job_name)
        _run_pending_jobs()
        successor = Task.objects.get(id=task.id).job_name

        stale.save(schedule_job=True, clean=False)

        self.assertEqual({successor}, self.scheduled_job_names())
        self.assertEqual(successor, Task.objects.get(id=task.id).job_name)

    def test_stale_task_save_does_not_roll_back_counters(self):
        task = task_factory(TaskType.CRON)
        stale = Task.objects.get(id=task.id)

        self.enqueue_scheduled_job(stale.job_name)
        _run_pending_jobs()

        stale.save(schedule_job=True, clean=False)

        reloaded = Task.objects.get(id=task.id)
        self.assertEqual(1, reloaded.successful_runs)
        self.assertIsNotNone(reloaded.last_successful_run)

    def test_reschedule_if_needed_is_a_noop_for_a_scheduled_task(self):
        task = task_factory(TaskType.CRON)
        successor = task.job_name

        self.assertFalse(Task.objects.get(id=task.id).reschedule_if_needed())

        self.assertEqual({successor}, self.scheduled_job_names())

    def test_reschedule_if_needed_schedules_an_unscheduled_task(self):
        task = task_factory(TaskType.CRON)
        task.unschedule()
        self.assertEqual(set(), self.scheduled_job_names())

        reloaded = Task.objects.get(id=task.id)
        self.assertTrue(reloaded.reschedule_if_needed())

        self.assertEqual({reloaded.job_name}, self.scheduled_job_names())

    def test_disabled_task_run_clears_the_job_name(self):
        task = task_factory(TaskType.CRON)
        first_run = task.job_name
        Task.objects.filter(id=task.id).update(enabled=False)

        self.enqueue_scheduled_job(first_run)
        _run_pending_jobs()

        task.refresh_from_db()
        self.assertIsNone(task.job_name)
        self.assertEqual(set(), self.scheduled_job_names())
