from scheduler.helpers.queues import get_queue
from scheduler.tests.jobs import test_job
from ..test_views.base import BaseTestCase
from ...redis_models import JobModel
from ...worker import create_worker
from ...worker.commands import send_command
from ...worker.commands.suspend_worker import SuspendWorkCommand


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
