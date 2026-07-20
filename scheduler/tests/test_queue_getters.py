import time

from redis.retry import Retry

from scheduler.helpers.queues.getters import _fail_fast_kwargs, _get_connection
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.types import Broker, QueueConfiguration


class TestFailFastKwargs(SchedulerBaseCase):
    def test_defaults_are_injected_when_absent(self):
        kwargs = _fail_fast_kwargs({})
        self.assertIsNone(kwargs["retry"])
        self.assertEqual(kwargs["socket_connect_timeout"], SCHEDULER_CONFIG.QUEUE_PROBE_SOCKET_CONNECT_TIMEOUT)

    def test_user_configured_values_are_respected(self):
        retry = Retry(None, 3)
        kwargs = _fail_fast_kwargs({"retry": retry, "socket_connect_timeout": 42})
        self.assertIs(kwargs["retry"], retry)
        self.assertEqual(kwargs["socket_connect_timeout"], 42)

    def test_does_not_mutate_input(self):
        original = {}
        _fail_fast_kwargs(original)
        self.assertEqual(original, {})


class TestGetConnectionFailFast(SchedulerBaseCase):
    """Covers https://github.com/django-commons/django-tasks-scheduler/issues/378:
    unreachable queues should fail fast when views probe every configured queue, and that
    should be controllable via SCHEDULER_CONFIG.FAIL_FAST_QUEUE_PROBING.
    """

    def setUp(self) -> None:
        super().setUp()
        self._orig_broker = SCHEDULER_CONFIG.BROKER
        self._orig_fail_fast = SCHEDULER_CONFIG.FAIL_FAST_QUEUE_PROBING
        # These tests exercise real redis-py connection construction/kwargs, regardless of
        # whether the suite is otherwise running against fakeredis.
        SCHEDULER_CONFIG.BROKER = Broker.REDIS

    def tearDown(self) -> None:
        SCHEDULER_CONFIG.BROKER = self._orig_broker
        SCHEDULER_CONFIG.FAIL_FAST_QUEUE_PROBING = self._orig_fail_fast
        super().tearDown()

    def test_fail_fast_true_applies_short_timeout_and_disables_retry(self):
        config = QueueConfiguration(HOST="localhost", PORT=1, DB=0)
        connection = _get_connection(config, fail_fast=True)
        kwargs = connection.connection_pool.connection_kwargs
        self.assertIsNone(kwargs["retry"])
        self.assertEqual(kwargs["socket_connect_timeout"], SCHEDULER_CONFIG.QUEUE_PROBE_SOCKET_CONNECT_TIMEOUT)

    def test_fail_fast_false_uses_default_connection_settings(self):
        config = QueueConfiguration(HOST="localhost", PORT=1, DB=0)
        connection = _get_connection(config, fail_fast=False)
        kwargs = connection.connection_pool.connection_kwargs
        self.assertIsNotNone(kwargs["retry"])

    def test_fail_fast_respects_user_configured_connection_kwargs(self):
        config = QueueConfiguration(HOST="localhost", PORT=1, DB=0, CONNECTION_KWARGS={"socket_connect_timeout": 7})
        connection = _get_connection(config, fail_fast=True)
        kwargs = connection.connection_pool.connection_kwargs
        self.assertEqual(kwargs["socket_connect_timeout"], 7)
        self.assertIsNone(kwargs["retry"])

    def test_fail_fast_probing_can_be_disabled_globally(self):
        SCHEDULER_CONFIG.FAIL_FAST_QUEUE_PROBING = False
        config = QueueConfiguration(HOST="localhost", PORT=1, DB=0)
        connection = _get_connection(config, fail_fast=True)
        kwargs = connection.connection_pool.connection_kwargs
        self.assertIsNotNone(kwargs["retry"])

    def test_unreachable_queue_with_fail_fast_does_not_retry_with_backoff(self):
        config = QueueConfiguration(HOST="localhost", PORT=1, DB=0)
        connection = _get_connection(config, fail_fast=True)
        start = time.time()
        with self.assertRaises(Exception):
            connection.ping()
        self.assertLess(time.time() - start, 1.0)
