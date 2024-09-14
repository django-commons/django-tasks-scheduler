import os
import uuid

from rq.job import Job
from scheduler.rq_classes import JobExecution
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.tools import create_worker
from . import test_settings  # noqa
from .. import settings


class TestWorker(SchedulerBaseCase):
    def test_create_worker__two_workers_same_queue(self):
        worker1 = create_worker("default", "django_tasks_scheduler_test")
        worker1.register_birth()
        worker2 = create_worker("default")
        worker2.register_birth()
        hostname = os.uname()[1]
        self.assertEqual(f"{hostname}-worker.1", worker1.name)
        self.assertEqual(f"{hostname}-worker.2", worker2.name)

    def test_create_worker__worker_with_queues_different_connection(self):
        with self.assertRaises(ValueError):
            create_worker("default", "test1")

    def test_create_worker__with_name(self):
        name = uuid.uuid4().hex
        worker1 = create_worker("default", name=name)
        self.assertEqual(name, worker1.name)

    def test_create_worker__with_name_containing_slash(self):
        name = uuid.uuid4().hex[-4:] + "/" + uuid.uuid4().hex[-4:]
        worker1 = create_worker("default", name=name)
        self.assertEqual(name.replace("/", "."), worker1.name)

    def test_create_worker__scheduler_interval(self):
        prev = settings.SCHEDULER_CONFIG.SCHEDULER_INTERVAL
        settings.SCHEDULER_CONFIG.SCHEDULER_INTERVAL = 1
        worker = create_worker("default")
        worker.work(burst=True)
        self.assertEqual(worker.scheduler.interval, 1)
        settings.SCHEDULER_CONFIG.SCHEDULER_INTERVAL = prev

    def test_get_worker_with_custom_job_class(self):
        # Test with string representation of job_class
        worker = create_worker("default", job_class="scheduler.rq_classes.JobExecution")
        self.assertTrue(issubclass(worker.job_class, Job))
        self.assertTrue(issubclass(worker.job_class, JobExecution))

    def test_get_worker_without_custom_job_class(self):
        worker = create_worker("default")
        self.assertTrue(issubclass(worker.job_class, JobExecution))
