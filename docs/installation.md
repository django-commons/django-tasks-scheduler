# Installation

1. Use pip to install:
   ```shell
   pip install django-tasks-scheduler
   ```

2. In `settings.py`, add `scheduler` to  `INSTALLED_APPS`:
   ```python
   INSTALLED_APPS = [
       # ...    
       'scheduler',
       # ...
   ]
   ```

3. Configure your queues.
   Add at least one Redis Queue to your `settings.py`.
   Note that the usage of `QueueConfiguration` is optional, you can use a simple dictionary, but `QueueConfiguration`
   helps preventing configuration errors.
   ```python
    import os
    from typing import Dict
    from scheduler.types import QueueConfiguration
       
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
    'with-sentinel': QueueConfiguration(
         SENTINELS= [('localhost', 26736), ('localhost', 26737)],
         MASTER_NAME= 'redismaster',
         DB= 0,
         USERNAME= 'redis-user',
         PASSWORD= 'secret',
         CONNECTION_KWARGS= {
         'ssl': True},
         SENTINEL_KWARGS= {
         'username': 'sentinel-user',
         'password': 'secret',
      }),
     'high': QueueConfiguration(URL=os.getenv('REDISTOGO_URL', 'redis://localhost:6379/0')),
     'low': QueueConfiguration(HOST='localhost', PORT=6379, DB=0, ASYNC=False), 
    }
   ```

4. Optional: Configure default values for queuing jobs from code:
   ```python
   from scheduler.types import SchedulerConfiguration, Broker

   SCHEDULER_CONFIG = SchedulerConfiguration(
    EXECUTIONS_IN_PAGE=20,
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
   ```

5. Add `scheduler.urls` to your django application `urls.py`:
   ```python
   from django.urls import path, include
   
   urlpatterns = [
       # ...
       path('scheduler/', include('scheduler.urls')),
   ]
   ```

6. Run migrations to generate django models
   ```shell
   python manage.py migrate
   ```

