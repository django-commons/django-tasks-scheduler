# Configure your django-tasks-scheduler

## settings.py

All default settings for scheduler can be in one dictionary in `settings.py`:

```python
import os
from typing import Dict
from scheduler.types import SchedulerConfiguration, Broker, QueueConfiguration

SCHEDULER_CONFIG = SchedulerConfiguration(
    EXECUTIONS_IN_PAGE=20,
    SCHEDULER_INTERVAL=10,
    BROKER=Broker.REDIS,
    CALLBACK_TIMEOUT=60,  # Callback timeout in seconds (success/failure/stopped)
    # Default values, can be overridden per task/job
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
SCHEDULER_QUEUES: Dict[str, QueueConfiguration] = {
    'default': QueueConfiguration(
        HOST='localhost',
        PORT=6379,
        USERNAME='some-user',
        PASSWORD='some-password',
        CONNECTION_KWARGS={  # Eventual additional Broker connection arguments
            'ssl_cert_reqs': 'required',
            'ssl': True,
        },
    ),
    'high': QueueConfiguration(URL=os.getenv('REDISTOGO_URL', 'redis://localhost:6379/0')),
    'low': QueueConfiguration(HOST='localhost', PORT=6379, DB=0, ASYNC=False),
}
```

### SCHEDULER_CONFIG: `EXECUTIONS_IN_PAGE`

Number of job executions to show in a page in a ScheduledJob admin view.

Default: `20`.

### SCHEDULER_CONFIG: `SCHEDULER_INTERVAL`

Default scheduler interval, a scheduler is a subprocess of a worker and
will check which job executions are pending.

Default: `10` (10 seconds).

### SCHEDULER_CONFIG: `BROKER`

### SCHEDULER_CONFIG: `CALLBACK_TIMEOUT`

### SCHEDULER_CONFIG: `DEFAULT_SUCCESS_TTL`

Default time to live for job execution result when it is successful.

Default: `600` (10 minutes).

### SCHEDULER_CONFIG: `DEFAULT_FAILURE_TTL`

Default time to live for job execution result when it is failed.

Default: `600` (10 minutes).

### SCHEDULER_CONFIG: `DEFAULT_JOB_TTL`

Default timeout for job info.

Default: `300` (5 minutes).

### SCHEDULER_CONFIG: `DEFAULT_JOB_TIMEOUT`

timeout (seconds) for a job.

Default: `300` (5 minutes).

### SCHEDULER_CONFIG: `DEFAULT_WORKER_TTL`

Time To Live (TTL) in seconds to keep worker information after last heartbeat.
Default: `600` (10 minutes).

### SCHEDULER_CONFIG: `DEFAULT_MAINTENANCE_TASK_INTERVAL`

The interval to run worker maintenance tasks in seconds.
Default: `600` 10 minutes.

### SCHEDULER_CONFIG: `DEFAULT_JOB_MONITORING_INTERVAL`

The interval to monitor jobs in seconds.

### SCHEDULER_CONFIG: `SCHEDULER_FALLBACK_PERIOD_SECS`

Period (secs) to wait before requiring to reacquire locks.

### SCHEDULER_CONFIG: `TOKEN_VALIDATION_METHOD`

Method to validate request `Authorization` header with.
Enables checking stats using API token.

Default: no tokens allowed.

### `SCHEDULER_QUEUES`

You can configure the queues to work with.
That way you can have different workers listening to different queues.

Different queues can use different redis servers/connections.
