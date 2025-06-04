from django.conf import settings

from scheduler.settings import conf_settings, SCHEDULER_CONFIG
from scheduler.tests.testtools import SchedulerBaseCase
from scheduler.types import Broker


class TestWorkerAdmin(SchedulerBaseCase):

    def test_scheduler_config_as_dict(self):
        self.assertEqual(SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE, 20)
        settings.SCHEDULER_CONFIG = dict(
            EXECUTIONS_IN_PAGE=1,
            SCHEDULER_INTERVAL=10,
            BROKER=Broker.REDIS,
            CALLBACK_TIMEOUT=60,  # Callback timeout in seconds (success/failure/stopped)
            # Default values, can be overriden per task/job
            DEFAULT_SUCCESS_TTL=10 * 60,  # Time To Live (TTL) in seconds to keep successful job results
            DEFAULT_FAILURE_TTL=365 * 24 * 60 * 60,  # Time To Live (TTL) in seconds to keep job failure information
            DEFAULT_JOB_TTL=10 * 60,  # Time To Live (TTL) in seconds to keep job information
            DEFAULT_JOB_TIMEOUT=5 * 60,  # timeout (seconds) for a job
            # General configuration values
            DEFAULT_WORKER_TTL=10 * 60,  # Time To Live (TTL) in seconds to keep worker information after last heartbeat
            DEFAULT_MAINTENANCE_TASK_INTERVAL=10 * 60,  # The interval to run maintenance tasks in seconds. 10 minutes.
            DEFAULT_JOB_MONITORING_INTERVAL=30,  # The interval to monitor jobs in seconds.
            SCHEDULER_FALLBACK_PERIOD_SECS=120,  # Period (secs) to wait before requiring to reacquire locks
        )
        conf_settings()
        self.assertEqual(SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE, 1)