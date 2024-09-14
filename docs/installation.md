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
   Add at least one Redis Queue to your `settings.py`:
   ```python
   import os
   SCHEDULER_QUEUES = {
     'default': {
         'HOST': 'localhost',
         'PORT': 6379,
        'DB': 0,
         'USERNAME': 'some-user',
         'PASSWORD': 'some-password',
         'DEFAULT_TIMEOUT': 360,
         'REDIS_CLIENT_KWARGS': {    # Eventual additional Redis connection arguments
             'ssl_cert_reqs': None,
         },
     },
     'with-sentinel': {
         'SENTINELS': [('localhost', 26736), ('localhost', 26737)],
         'MASTER_NAME': 'redismaster',
         'DB': 0,
         # Redis username/password
         'USERNAME': 'redis-user',
         'PASSWORD': 'secret',
         'SOCKET_TIMEOUT': 0.3,
         'CONNECTION_KWARGS': {  # Eventual additional Redis connection arguments
             'ssl': True
         },
         'SENTINEL_KWARGS': {    # Eventual Sentinel connection arguments
             # If Sentinel also has auth, username/password can be passed here
             'username': 'sentinel-user',
             'password': 'secret',
         },
     },
     'high': {
         'URL': os.getenv('REDISTOGO_URL', 'redis://localhost:6379/0'), # If you're on Heroku
         'DEFAULT_TIMEOUT': 500,
     },
     'low': {
         'HOST': 'localhost',
         'PORT': 6379,
         'DB': 0,
     }
   }
   ```
   
4. Optional: Configure default values for queuing jobs from code:
   ```python
   SCHEDULER_CONFIG = {
    'EXECUTIONS_IN_PAGE': 20,
    'DEFAULT_RESULT_TTL': 500,
    'DEFAULT_TIMEOUT': 300,  # 5 minutes
    'SCHEDULER_INTERVAL': 10,  # 10 seconds
    'BROKER': 'redis', # 
   }
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

