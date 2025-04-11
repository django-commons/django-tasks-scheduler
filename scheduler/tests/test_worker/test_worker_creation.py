import os
import uuid

from scheduler import settings
from scheduler.redis_models import WorkerModel
from scheduler.worker import create_worker
from scheduler.tests import test_settings  # noqa
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.worker.worker import QueueConnectionDiscrepancyError


class TestWorker(SchedulerBaseCase):
    def test_create_worker__two_workers_same_queue(self):
        worker1 = create_worker("default", "django_tasks_scheduler_test")
        worker1.worker_start()
        worker2 = create_worker("default")
        worker2.worker_start()
        hostname = os.uname()[1]
        self.assertEqual(f"{hostname}-worker.1", worker1.name)
        self.assertEqual(f"{hostname}-worker.2", worker2.name)

    def test_create_worker__worker_with_queues_different_connection(self):
        with self.assertRaises(QueueConnectionDiscrepancyError):
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
        worker = create_worker("default", name="test", burst=True, with_scheduler=True)
        worker.bootstrap()
        self.assertEqual(worker.name, "test")
        self.assertEqual(worker.scheduler.interval, 1)
        settings.SCHEDULER_CONFIG.SCHEDULER_INTERVAL = prev
        worker.teardown()

    def test_create_worker__cleanup(self):
        worker = create_worker("default", name="test", burst=True, with_scheduler=False)
        worker.bootstrap()
        worker.connection.delete(WorkerModel.key_for(worker.name))
        all_names = WorkerModel.all_names(worker.connection)
        self.assertIn(worker.name, all_names)
        # act
        WorkerModel.cleanup(worker.connection, "default")
        # assert
        all_names = WorkerModel.all_names(worker.connection)
        self.assertNotIn(worker.name, all_names)
