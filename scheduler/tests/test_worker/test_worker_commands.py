import json
from threading import Thread
from time import sleep
from unittest import mock

from scheduler.helpers.queues import get_queue
from scheduler.tests.jobs import test_job, two_seconds_job
from ..test_views.base import BaseTestCase
from ...helpers.callback import Callback
from ...redis_models import JobModel, JobStatus, WorkerModel
from ...worker import create_worker
from ...worker.commands import send_command, StopJobCommand
from ...worker.commands.suspend_worker import SuspendWorkCommand


def _callback_func():
    pass


def callback_func():
    pass


class WorkerCommandsTest(BaseTestCase):
    def test_stop_worker_command__green(self):
        # Arrange
        worker_name = "test"
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job)
        self.assertTrue(job.is_queued)
        worker = create_worker("default", name=worker_name, burst=True, with_scheduler=False)
        worker.worker_start()
        # Act
        send_command(queue.connection, SuspendWorkCommand(worker_name=worker_name))
        worker.work()

        # Assert
        self.assertTrue(job.is_queued)
        self.assertTrue(worker._model.is_suspended)

    def test_stop_worker_command__bad_worker_name(self):
        # Arrange
        worker_name = "test"
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(test_job)
        self.assertTrue(job.is_queued)
        worker = create_worker("default", name=worker_name, burst=True, with_scheduler=False)
        worker.bootstrap()
        # Act
        send_command(queue.connection, SuspendWorkCommand(worker_name=worker_name + "1"))
        worker.work()

        # Assert
        self.assertFalse(worker._model.is_suspended)
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertFalse(job.is_queued)

    @mock.patch("scheduler.redis_models.job.JobModel.call_stopped_callback")
    def test_stop_job_command__success(self, mock_stopped_callback):
        # Arrange
        worker_name = "test"
        queue = get_queue("default")
        job = queue.create_and_enqueue_job(two_seconds_job, on_stopped=Callback(callback_func))
        self.assertTrue(job.is_queued)
        worker = create_worker("default", name=worker_name, burst=True, with_scheduler=False)
        worker.bootstrap()

        # Act
        t = Thread(target=worker.work, args=(0,), name="worker-thread")
        t.start()
        sleep(0.1)
        command = StopJobCommand(worker_name=worker_name, job_name=job.name)
        command_payload = json.dumps(command.command_payload())
        worker._command_listener.handle_payload({"data": command_payload})
        worker.monitor_job_execution_process(job, queue)

        # Assert
        job = JobModel.get(job.name, connection=queue.connection)
        worker = WorkerModel.get(worker.name, connection=queue.connection)
        self.assertEqual(worker.stopped_job_name, job.name)
        self.assertIsNone(worker.current_job_name)
        self.assertEqual(job.status, JobStatus.STOPPED)
        t.join()
        mock_stopped_callback.assert_called()
