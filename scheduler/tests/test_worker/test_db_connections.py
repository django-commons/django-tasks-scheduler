"""The worker process must hold no open Django DB connection when it forks a job child.

A child that inherits one shares the parent's socket. See scheduler.helpers.db for the
full mechanism and what it costs in production.

Two things shape how these tests are written:

* django.test.TestCase wraps each test in a transaction, which keeps a connection open for
  the whole test and makes closing one impossible to observe, so these use
  TransactionTestCase.
* Django deliberately ignores close() on an in-memory database, which is what the test
  database is, so `connection.connection is None` can never be observed here. Instead each
  test records whether a connection was live at the moment the worker closed it, which is
  the state the fork depends on.
"""

import threading
from unittest import mock

from django.db import connections
from django.test import TransactionTestCase

from scheduler.helpers.callback import Callback
from scheduler.helpers.db import close_db_connections
from scheduler.helpers.queues import get_queue
from scheduler.helpers.utils import current_timestamp
from scheduler.models import Task, TaskType
from scheduler.models.task import failure_callback
from scheduler.tests import conf  # noqa: F401
from scheduler.tests.jobs import test_job
from scheduler.tests.testtools import task_factory
from scheduler.worker import create_worker


class WorkerDbConnectionTestCase(TransactionTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.queue = get_queue("default")
        self.queue.connection.flushall()
        self.events: list[str] = []

    def _worker(self, name: str):
        worker = create_worker("default", name=name, with_scheduler=False)
        worker.worker_start()
        return worker

    def _watch_closes(self):
        """Record each close of the default connection, and whether it had a live socket.

        The real close still runs. "live" is the part that matters: it says the worker was
        holding a usable connection at that moment, which is what a child would inherit.
        """
        real_close = connections["default"].close

        def close_and_record() -> None:
            self.events.append("closed-a-live-connection" if connections["default"].connection else "closed-nothing")
            real_close()

        return mock.patch.object(connections["default"], "close", close_and_record)

    def _fork_returning_a_child_pid(self):
        """Stand in for os.fork, returning a pid so the parent branch runs.

        A real fork here would duplicate the test runner.
        """

        def fork() -> int:
            self.events.append("forked")
            return 4242

        return mock.patch("scheduler.worker.worker.os.fork", fork)


class TestForkingAJobExecutionProcess(WorkerDbConnectionTestCase):
    def test_the_workers_db_connection_is_closed_before_the_fork(self):
        job = self.queue.create_and_enqueue_job(test_job)
        worker = self._worker("test-fork-db-connections")
        Task.objects.count()  # the worker's thread now owns a real DB connection

        with self._watch_closes(), self._fork_returning_a_child_pid():
            worker.fork_job_execution_process(job, self.queue)

        self.assertEqual(
            ["closed-a-live-connection", "forked"],
            self.events,
            "the child inherits the parent's connection object and its socket, and every child "
            "after the first then fails on its first query",
        )

    def test_a_connection_that_will_not_close_does_not_stop_the_worker_forking(self):
        # Closing a connection whose socket is already broken can raise, and that is one of
        # the states this is here to clear.
        job = self.queue.create_and_enqueue_job(test_job)
        worker = self._worker("test-fork-db-close-raises")
        Task.objects.count()

        with (
            mock.patch.object(connections["default"], "close", side_effect=OSError("broken socket")),
            self._fork_returning_a_child_pid(),
        ):
            worker.fork_job_execution_process(job, self.queue)

        self.assertEqual(["forked"], self.events, "a connection that will not close must not stop the job running")


class TestMaintenanceThatRunsAFailureCallback(WorkerDbConnectionTestCase):
    def _enqueue_an_abandoned_job_for(self, task: Task):
        """Enqueue a job for `task`, registered as active long enough ago to count as abandoned."""
        job = self.queue.create_and_enqueue_job(
            test_job,
            timeout=60,
            on_failure=Callback(failure_callback),
            task_type=task.task_type,
            scheduled_task_id=task.id,
        )
        self.queue.active_job_registry.add(self.queue.connection, job.name, current_timestamp() - 3600)
        return job

    def test_the_connection_the_failure_callback_opened_is_closed_again(self):
        task = task_factory(TaskType.ONCE)
        self._enqueue_an_abandoned_job_for(task)
        worker = self._worker("test-maintenance-db-connections")
        connections["default"].close()

        with self._watch_closes():
            worker.run_maintenance_tasks()

        task.refresh_from_db()
        self.assertEqual(1, task.failed_runs, "test setup: the ORM failure callback did not run")
        self.assertEqual(
            ["closed-a-live-connection"],
            self.events,
            "the maintenance pass ran a Django ORM failure callback in the worker process and left "
            "its connection open, so every job child forked afterwards inherits it",
        )

    def test_the_connection_is_closed_even_when_the_failure_callback_raises(self):
        task = task_factory(TaskType.ONCE)
        self._enqueue_an_abandoned_job_for(task)
        worker = self._worker("test-maintenance-db-raises")

        with (
            self._watch_closes(),
            mock.patch("scheduler.models.task.mail_admins", side_effect=ValueError("mail server down")),
            self.assertRaises(ValueError),
        ):
            worker.run_maintenance_tasks()

        self.assertEqual(
            ["closed-a-live-connection"],
            self.events,
            "clean_registries re-raises a failing failure callback, and the connection that callback "
            "opened has to be released either way",
        )


class TestCloseDbConnections(WorkerDbConnectionTestCase):
    def test_another_threads_connection_is_left_open_and_usable(self):
        # The scheduler runs in its own thread off its own connection. Django's connection
        # handler is thread-local and this fix depends on that: closing the worker's
        # connections must not disturb the scheduler mid-query.
        scheduler_has_connected = threading.Event()
        worker_has_closed = threading.Event()
        scheduler: dict = {}

        def scheduler_thread() -> None:
            try:
                Task.objects.count()
                scheduler["connection_before"] = connections["default"].connection
                scheduler_has_connected.set()
                worker_has_closed.wait(timeout=10)
                scheduler["connection_after"] = connections["default"].connection
                scheduler["task_count"] = Task.objects.count()
            except BaseException as e:
                scheduler["error"] = e  # reported as a test failure below
            finally:
                scheduler_has_connected.set()
                connections["default"].close()

        task_factory(TaskType.ONCE)
        thread = threading.Thread(target=scheduler_thread, name="scheduler-thread")
        thread.start()
        self.assertTrue(scheduler_has_connected.wait(timeout=10), "test setup: scheduler thread never started")

        Task.objects.count()  # the worker's own connection
        with self._watch_closes():
            close_db_connections()
        worker_has_closed.set()
        thread.join(timeout=10)

        self.assertIsNone(scheduler.get("error"), f"scheduler thread failed: {scheduler.get('error')}")
        self.assertEqual(["closed-a-live-connection"], self.events, "the worker's own connection was not closed")
        self.assertIs(
            scheduler["connection_before"],
            scheduler["connection_after"],
            "the scheduler thread's connection should be untouched",
        )
        self.assertEqual(1, scheduler["task_count"], "the scheduler thread should still be able to query")
