import dataclasses

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from scheduler.settings import conf_settings
from scheduler.tests import conf  # noqa: F401  -- applies FAKEREDIS=True when running this module alone
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.types import Broker, SchedulerConfiguration

_MISSING = object()


class TestWorkerAdmin(SchedulerBaseCase):
    def setUp(self):
        from scheduler.settings import SCHEDULER_CONFIG

        # `conf_settings()` has two ways of applying a config, and undoing it needs both covered:
        # a `SchedulerConfiguration` rebinds `scheduler.settings.SCHEDULER_CONFIG` to a new object,
        # while a dict calls `setattr()` on the live one. Helper modules import that object by
        # reference (e.g. `scheduler.helpers.queues.getters`), so rebinding the module global alone
        # restores nothing -- the field values have to be written back onto the original object.
        self.old_settings = SCHEDULER_CONFIG
        self.old_config_values = {
            f.name: getattr(SCHEDULER_CONFIG, f.name) for f in dataclasses.fields(SCHEDULER_CONFIG)
        }
        self.old_django_setting = getattr(settings, "SCHEDULER_CONFIG", _MISSING)

    def tearDown(self):
        from scheduler import settings as scheduler_settings

        scheduler_settings.SCHEDULER_CONFIG = self.old_settings
        for key, value in self.old_config_values.items():
            setattr(self.old_settings, key, value)
        if self.old_django_setting is _MISSING:
            del settings.SCHEDULER_CONFIG
        else:
            settings.SCHEDULER_CONFIG = self.old_django_setting

    def test_scheduler_config_as_dict(self):
        from scheduler.settings import SCHEDULER_CONFIG

        settings.SCHEDULER_CONFIG = {
            "EXECUTIONS_IN_PAGE": SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE + 1,
            "SCHEDULER_INTERVAL": SCHEDULER_CONFIG.SCHEDULER_INTERVAL + 1,
            "BROKER": Broker.REDIS,
            "CALLBACK_TIMEOUT": SCHEDULER_CONFIG.SCHEDULER_INTERVAL + 1,
            "DEFAULT_SUCCESS_TTL": SCHEDULER_CONFIG.DEFAULT_SUCCESS_TTL + 1,
            "DEFAULT_FAILURE_TTL": SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL + 1,
            "DEFAULT_JOB_TTL": SCHEDULER_CONFIG.DEFAULT_JOB_TTL + 1,
            "DEFAULT_JOB_TIMEOUT": SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT + 1,
            # General configuration values
            "DEFAULT_WORKER_TTL": SCHEDULER_CONFIG.DEFAULT_WORKER_TTL + 1,
            "DEFAULT_MAINTENANCE_TASK_INTERVAL": SCHEDULER_CONFIG.DEFAULT_MAINTENANCE_TASK_INTERVAL + 1,
            "DEFAULT_JOB_MONITORING_INTERVAL": SCHEDULER_CONFIG.DEFAULT_JOB_MONITORING_INTERVAL + 1,
            "SCHEDULER_FALLBACK_PERIOD_SECS": SCHEDULER_CONFIG.SCHEDULER_FALLBACK_PERIOD_SECS + 1,
        }
        conf_settings()
        from scheduler.settings import SCHEDULER_CONFIG

        for key, value in settings.SCHEDULER_CONFIG.items():
            self.assertEqual(getattr(SCHEDULER_CONFIG, key), value)

    def test_scheduler_config_as_data_class(self):
        from scheduler.settings import SCHEDULER_CONFIG

        self.assertEqual(SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE, 20)
        settings.SCHEDULER_CONFIG = SchedulerConfiguration(
            EXECUTIONS_IN_PAGE=1,
            SCHEDULER_INTERVAL=60,
            BROKER=Broker.REDIS,
            CALLBACK_TIMEOUT=1111,
            DEFAULT_SUCCESS_TTL=1111,
            DEFAULT_FAILURE_TTL=111111,
            DEFAULT_JOB_TTL=1111,
            DEFAULT_JOB_TIMEOUT=11111,
            # General configuration values
            DEFAULT_WORKER_TTL=11111,
            DEFAULT_MAINTENANCE_TASK_INTERVAL=111,
            DEFAULT_JOB_MONITORING_INTERVAL=1111,
            SCHEDULER_FALLBACK_PERIOD_SECS=1111,
        )
        conf_settings()
        from scheduler.settings import SCHEDULER_CONFIG

        for key, value in dataclasses.asdict(settings.SCHEDULER_CONFIG).items():
            self.assertEqual(getattr(SCHEDULER_CONFIG, key), value)

    def test_scheduler_config_as_dict_bad_param(self):
        settings.SCHEDULER_CONFIG = {
            "EXECUTIONS_IN_PAGE": 1,
            "SCHEDULER_INTERVAL": 60,
            "BROKER": Broker.REDIS,
            "CALLBACK_TIMEOUT": 1111,
            "DEFAULT_SUCCESS_TTL": 1111,
            "DEFAULT_FAILURE_TTL": 111111,
            "DEFAULT_JOB_TTL": 1111,
            "DEFAULT_JOB_TIMEOUT": 11111,
            # General configuration values
            "DEFAULT_WORKER_TTL": 11111,
            "DEFAULT_MAINTENANCE_TASK_INTERVAL": 111,
            "DEFAULT_JOB_MONITORING_INTERVAL": 1111,
            "SCHEDULER_FALLBACK_PERIOD_SECS": 1111,
            "BAD_PARAM": "bad_value",  # This should raise an error
        }
        self.assertRaises(ImproperlyConfigured, conf_settings)


class TestSchedulerConfigIsolation(SchedulerBaseCase):
    """A test that changes SCHEDULER_CONFIG must leave the live config object as it found it.

    `TestWorkerAdmin` used to save the config object by reference and restore it by rebinding the
    module global, which does not undo the in-place `setattr()` that `conf_settings()` performs for
    a dict config. `BROKER` therefore stayed on whatever the last such test set, silently disabling
    `FAKEREDIS=True` for every test that ran afterwards -- invisible in CI, where a real broker is
    running on the default port anyway.
    """

    def _assert_no_leak(self, method_name: str) -> None:
        from scheduler.helpers.queues import getters
        from scheduler.settings import SCHEDULER_CONFIG

        original_broker = SCHEDULER_CONFIG.BROKER
        original_page_size = SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE
        original_django_setting = getattr(settings, "SCHEDULER_CONFIG", _MISSING)

        case = TestWorkerAdmin(method_name)
        case.setUp()
        try:
            getattr(case, method_name)()
        finally:
            case.tearDown()

        self.assertEqual(original_broker, SCHEDULER_CONFIG.BROKER)
        self.assertEqual(original_page_size, SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE)
        # Helper modules imported the config object by reference, so check that binding too.
        self.assertEqual(original_broker, getters.SCHEDULER_CONFIG.BROKER)
        # The django setting itself is assigned directly by these tests and must be put back.
        self.assertIs(original_django_setting, getattr(settings, "SCHEDULER_CONFIG", _MISSING))

    def test_dict_config_does_not_leak(self):
        self._assert_no_leak("test_scheduler_config_as_dict")

    def test_data_class_config_does_not_leak(self):
        self._assert_no_leak("test_scheduler_config_as_data_class")
