import dataclasses

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from scheduler.settings import conf_settings
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.types import Broker, SchedulerConfiguration


class TestWorkerAdmin(SchedulerBaseCase):

    def setUp(self):
        from scheduler.settings import SCHEDULER_CONFIG
        self.old_settings = SCHEDULER_CONFIG

    def tearDown(self):
        from scheduler import settings as scheduler_settings
        scheduler_settings.SCHEDULER_CONFIG = self.old_settings

    def test_scheduler_config_as_dict(self):
        from scheduler.settings import SCHEDULER_CONFIG
        settings.SCHEDULER_CONFIG = dict(
            EXECUTIONS_IN_PAGE=SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE + 1,
            SCHEDULER_INTERVAL=SCHEDULER_CONFIG.SCHEDULER_INTERVAL + 1,
            BROKER=Broker.REDIS,
            CALLBACK_TIMEOUT=SCHEDULER_CONFIG.SCHEDULER_INTERVAL + 1,

            DEFAULT_SUCCESS_TTL=SCHEDULER_CONFIG.DEFAULT_SUCCESS_TTL + 1,
            DEFAULT_FAILURE_TTL=SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL + 1,
            DEFAULT_JOB_TTL=SCHEDULER_CONFIG.DEFAULT_JOB_TTL + 1,
            DEFAULT_JOB_TIMEOUT=SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT + 1,
            # General configuration values
            DEFAULT_WORKER_TTL=SCHEDULER_CONFIG.DEFAULT_WORKER_TTL + 1,
            DEFAULT_MAINTENANCE_TASK_INTERVAL=SCHEDULER_CONFIG.DEFAULT_MAINTENANCE_TASK_INTERVAL + 1,
            DEFAULT_JOB_MONITORING_INTERVAL=SCHEDULER_CONFIG.DEFAULT_JOB_MONITORING_INTERVAL + 1,
            SCHEDULER_FALLBACK_PERIOD_SECS=SCHEDULER_CONFIG.SCHEDULER_FALLBACK_PERIOD_SECS + 1,
        )
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
        settings.SCHEDULER_CONFIG = dict(
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
            BAD_PARAM='bad_value',  # This should raise an error
        )
        self.assertRaises(ImproperlyConfigured, conf_settings)
