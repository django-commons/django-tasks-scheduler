# Configure your django-tasks-scheduler

## settings.py

All default settings for scheduler can be in one dictionary in `settings.py`:

```python
from typing import Dict

from scheduler.configuration_types import SchedulerConfig, Broker, QueueConfiguration
from scheduler.helpers.timeouts import UnixSignalDeathPenalty

SCHEDULER_CONFIG = SchedulerConfig(
    EXECUTIONS_IN_PAGE=20,
    SCHEDULER_INTERVAL=10,
    BROKER=Broker.REDIS,
    CALLBACK_TIMEOUT=60,

    DEFAULT_RESULT_TTL=500,
    DEFAULT_FAILURE_TTL=365 * 24 * 60 * 60,
    DEFAULT_JOB_TIMEOUT=300,
    DEFAULT_WORKER_TTL=420,

    DEFAULT_MAINTENANCE_TASK_INTERVAL=10 * 60,
    DEFAULT_JOB_MONITORING_INTERVAL=30,
    SCHEDULER_FALLBACK_PERIOD_SECS=120,
    DEATH_PENALTY_CLASS=UnixSignalDeathPenalty,
)
SCHEDULER_QUEUES: Dict[str, QueueConfiguration] = {
    'default': QueueConfiguration(
        HOST='localhost',
        PORT=6379,
        USERNAME='some-user',
        PASSWORD='some-password',
        CONNECTION_KWARGS={  # Eventual additional Broker connection arguments
            'ssl_cert_reqs': 'required',
            'ssl':True,
        },
    ),
    'high': QueueConfiguration(URL=os.getenv('REDISTOGO_URL', 'redis://localhost:6379/0')),
    'low': QueueConfiguration(HOST='localhost', PORT=6379, DB=0, ASYNC=False),
}
```

### SCHEDULER_CONFIG: `EXECUTIONS_IN_PAGE`

Number of job executions to show in a page in a ScheduledJob admin view.

Default: `20`.

### SCHEDULER_CONFIG: `DEFAULT_RESULT_TTL`

Default time to live for job execution result.

Default: `600` (10 minutes).

### SCHEDULER_CONFIG: `DEFAULT_TIMEOUT`

Default timeout for job when it is not mentioned in queue.
Default: `300` (5 minutes).

### SCHEDULER_CONFIG: `SCHEDULER_INTERVAL`

Default scheduler interval, a scheduler is a subprocess of a worker and
will check which job executions are pending.

Default: `10` (10 seconds).

### SCHEDULER_CONFIG: `TOKEN_VALIDATION_METHOD`

Method to validate request `Authorization` header with.
Enables checking stats using API token.

Default: no tokens allowed.

### SCHEDULER_CONFIG: `BROKER`

Broker driver to use for the scheduler. Can be `redis` or `valkey` or `fakeredis`.

Default: `redis`.

### `SCHEDULER_QUEUES`

You can configure the queues to work with.
That way you can have different workers listening to different queues.

Different queues can use different redis servers/connections.
