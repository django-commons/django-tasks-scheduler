from time import sleep

from django.test import tag

from scheduler.helpers.queues import get_queue
from scheduler.redis_models import JobStatus, JobModel, WorkerModel
from scheduler.tests.jobs import long_job, two_seconds_job
from .. import testtools
from ..test_views.base import BaseTestCase
from ...worker.commands import KillWorkerCommand, send_command, StopJobCommand


@tag("multiprocess")
class WorkerCommandsTest(BaseTestCase):
    def test_kill_job_command__current_job(self):
        # Arrange
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(long_job)
        self.assertTrue(job.is_queued)
        process, worker_name = testtools.run_worker_in_process("django_tasks_scheduler_test")
        sleep(0.1)
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertEqual(JobStatus.STARTED, job.status)

        # Act
        send_command(queue.connection, StopJobCommand(worker_name=worker_name, job_name=job.name))

        # Assert

        process.terminate()
        process.join(2)
        process.kill()

        job = JobModel.get(job.name, connection=queue.connection)
        worker_model = WorkerModel.get(worker_name, connection=queue.connection)
        self.assertEqual(job.name, worker_model.stopped_job_name)
        self.assertEqual(job.name, worker_model.current_job_name)
        self.assertEqual(0, worker_model.completed_jobs)
        self.assertEqual(0, worker_model.failed_job_count)
        self.assertEqual(0, worker_model.successful_job_count)
        self.assertEqual(JobStatus.STOPPED, job.status)
        self.assertNotIn(job.name, queue.queued_job_registry.all())

    def test_kill_job_command__different_job(self):
        # Arrange
        queue = get_queue("django_tasks_scheduler_test")
        job = queue.create_and_enqueue_job(two_seconds_job)
        self.assertTrue(job.is_queued)
        process, worker_name = testtools.run_worker_in_process("django_tasks_scheduler_test")
        sleep(0.2)
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertEqual(JobStatus.STARTED, job.status)

        # Act
        send_command(queue.connection, StopJobCommand(worker_name=worker_name, job_name=job.name + "1"))
        sleep(0.1)
        process.kill()
        process.join()
        # Assert
        job = JobModel.get(job.name, connection=queue.connection)
        self.assertEqual(JobStatus.STARTED, job.status)
        self.assertNotIn(job.name, queue.queued_job_registry.all())
        worker_model = WorkerModel.get(worker_name, connection=queue.connection)
        self.assertEqual(0, worker_model.completed_jobs)
        self.assertEqual(0, worker_model.failed_job_count)
        self.assertEqual(0, worker_model.successful_job_count)
        self.assertIsNone(worker_model.stopped_job_name)
        self.assertEqual(job.name, worker_model.current_job_name)

    def test_kill_worker_command(self):
        queue = get_queue("django_tasks_scheduler_test")
        process, worker_name = testtools.run_worker_in_process("django_tasks_scheduler_test")
        sleep(0.1)
        # act
        send_command(queue.connection, KillWorkerCommand(worker_name=worker_name))
        # assert
        sleep(0.2)
        process.kill()
        process.join()
        worker_model = WorkerModel.get(worker_name, connection=queue.connection)
        self.assertEqual(0, worker_model.completed_jobs)
        self.assertEqual(0, worker_model.failed_job_count)
        self.assertEqual(0, worker_model.successful_job_count)
        self.assertIsNotNone(worker_model.shutdown_requested_date)
