import logging

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

logger = logging.getLogger("scheduler")

QUEUES = dict()
SCHEDULER_CONFIG = dict()


def conf_settings():
    global QUEUES
    global SCHEDULER_CONFIG

    QUEUES = getattr(settings, 'SCHEDULER_QUEUES', None)
    if QUEUES is None:
        logger.warning('Configuration using RQ_QUEUES is deprecated. Use SCHEDULER_QUEUES instead')
        QUEUES = getattr(settings, 'RQ_QUEUES', None)
    if QUEUES is None:
        raise ImproperlyConfigured("You have to define SCHEDULER_QUEUES in settings.py")

    SCHEDULER_CONFIG = {
        'EXECUTIONS_IN_PAGE': 20,
        'DEFAULT_RESULT_TTL': 600,  # 10 minutes
        'DEFAULT_TIMEOUT': 300,  # 5 minutes
        'SCHEDULER_INTERVAL': 10,  # 10 seconds
        'FAKEREDIS': False,  # For testing purposes
    }
    user_settings = getattr(settings, 'SCHEDULER_CONFIG', {})
    SCHEDULER_CONFIG.update(user_settings)


conf_settings()


def get_config(key: str, default=None):
    return SCHEDULER_CONFIG.get(key, None)
