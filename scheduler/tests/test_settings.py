from django.conf import settings

from scheduler.settings import conf_settings, SCHEDULER_CONFIG
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.types import Broker


class TestWorkerAdmin(SchedulerBaseCase):

    def test_scheduler_config_as_dict(self):
        self.assertEqual(SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE, 20)
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
        )
        conf_settings()
        for key, value in settings.SCHEDULER_CONFIG.items():
            self.assertEqual(getattr(SCHEDULER_CONFIG, key), value)
