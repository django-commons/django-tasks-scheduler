import socket
import uuid
from unittest import mock

from scheduler import settings
from scheduler.helpers.queues.getters import get_queue_connection
from scheduler.redis_models import WorkerModel
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.types import QueueConfiguration
from scheduler.worker import create_worker
from scheduler.tests import conf  # noqa
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.worker.worker import QueueConnectionDiscrepancyError


class TestWorker(SchedulerBaseCase):
    def test_create_worker__two_workers_same_queue(self):
        worker1 = create_worker("default", "django_tasks_scheduler_test")
        worker1.worker_start()
        worker2 = create_worker("default")
        worker2.worker_start()
        hostname = socket.gethostname()
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

    def test_connection__forces_socket_timeout_when_not_user_configured(self):
        # The default queue does not configure a socket_timeout, so the worker must force a value large enough
        # for its blocking dequeue (regression test for issue #375 - redis-py >= 8 injects a small default).
        worker = create_worker("default", name="test")
        socket_timeout = worker.connection.connection_pool.connection_kwargs.get("socket_timeout")
        self.assertEqual(SCHEDULER_CONFIG.DEFAULT_WORKER_TTL - 5, socket_timeout)

    def test_connection__respects_user_configured_socket_timeout(self):
        # When the user explicitly sets a socket_timeout via CONNECTION_KWARGS, the worker must not override it.
        worker = create_worker("default", name="test")
        connection = get_queue_connection("default")
        connection.connection_pool.connection_kwargs["socket_timeout"] = 42
        user_config = QueueConfiguration(URL="redis://localhost:6379/0", CONNECTION_KWARGS={"socket_timeout": 42})
        with (
            mock.patch("scheduler.worker.worker.get_queue_connection", return_value=connection),
            mock.patch("scheduler.worker.worker.get_queue_configuration", return_value=user_config),
        ):
            socket_timeout = worker.connection.connection_pool.connection_kwargs.get("socket_timeout")
        self.assertEqual(42, socket_timeout)

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
