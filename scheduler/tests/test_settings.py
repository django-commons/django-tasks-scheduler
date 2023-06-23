import os

from django.conf import settings

from scheduler.settings import conf_settings

settings.SCHEDULER_QUEUES = {
    'default': {'HOST': 'localhost', 'PORT': 6379, 'DB': 0, 'DEFAULT_TIMEOUT': 500},
    'test': {
        'HOST': 'localhost',
        'PORT': 1,
        'DB': 1,
    },
    'sentinel': {
        'SENTINELS': [('localhost', 26736), ('localhost', 26737)],
        'MASTER_NAME': 'testmaster',
        'DB': 1,
        'USERNAME': 'redis-user',
        'PASSWORD': 'secret',
        'SOCKET_TIMEOUT': 10,
        'SENTINEL_KWARGS': {},
    },
    'test1': {
        'HOST': 'localhost',
        'PORT': 1,
        'DB': 1,
        'DEFAULT_TIMEOUT': 400,
        'QUEUE_CLASS': 'django_rq.tests.fixtures.DummyQueue',
    },
    'test2': {
        'HOST': 'localhost',
        'PORT': 1,
        'DB': 1,
    },
    'test3': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 1,
    },
    'async': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 1,
        'ASYNC': False,
    },
    'url': {
        'URL': 'redis://username:password@host:1234/',
        'DB': 4,
    },
    'url_with_db': {
        'URL': 'redis://username:password@host:1234/5',
    },
    'url_default_db': {
        'URL': 'redis://username:password@host:1234',
    },
    'django_rq_scheduler_test': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
    },
    'scheduler_scheduler_active_test': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
        'ASYNC': False,
    },
    'scheduler_scheduler_inactive_test': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
        'ASYNC': False,
    },
    'worker_scheduler_active_test': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
        'ASYNC': False,
    },
    'worker_scheduler_inactive_test': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
        'ASYNC': False,
    },
    'django_rq_scheduler_test2': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
    },
    'test_scheduler': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
        'DEFAULT_TIMEOUT': 400,
    },
}
settings.SCHEDULER_CONFIG = dict(
    FAKEREDIS=(os.getenv('FAKEREDIS', 'False') == 'True'),
)
conf_settings()
