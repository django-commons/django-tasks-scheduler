# Configure your django-tasks-scheduler

## settings.py

All default settings for scheduler can be in one dictionary in `settings.py`:

```python
SCHEDULER_CONFIG = {
    'EXECUTIONS_IN_PAGE': 20,
    'DEFAULT_RESULT_TTL': 500,
    'DEFAULT_TIMEOUT': 300,  # 5 minutes
    'SCHEDULER_INTERVAL': 10,  # 10 seconds
    'BROKER': 'redis',
}
SCHEDULER_QUEUES = {
    'default': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
        'USERNAME': 'some-user',
        'PASSWORD': 'some-password',
        'DEFAULT_TIMEOUT': 360,
        'CLIENT_KWARGS': {  # Eventual additional Redis connection arguments
            'ssl_cert_reqs': None,
        },
        'TOKEN_VALIDATION_METHOD': None,  # Method to validate auth-header
    },
    'high': {
        'URL': os.getenv('REDISTOGO_URL', 'redis://localhost:6379/0'),  # If you're on Heroku
        'DEFAULT_TIMEOUT': 500,
    },
    'low': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
    }
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
