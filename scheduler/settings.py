import logging
from typing import List, Dict

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from scheduler.types import SchedulerConfiguration, QueueConfiguration

logger = logging.getLogger("scheduler")
logging.basicConfig(level=logging.DEBUG)

_QUEUES: Dict[str, QueueConfiguration] = dict()
SCHEDULER_CONFIG: SchedulerConfiguration = SchedulerConfiguration()


class QueueNotFoundError(Exception):
    pass


def conf_settings():
    global _QUEUES
    global SCHEDULER_CONFIG

    app_queues = getattr(settings, "SCHEDULER_QUEUES", None)
    if app_queues is None or not isinstance(app_queues, dict):
        raise ImproperlyConfigured("You have to define SCHEDULER_QUEUES in settings.py as dict")

    for queue_name, queue_config in app_queues.items():
        if isinstance(queue_config, QueueConfiguration):
            _QUEUES[queue_name] = queue_config
        elif isinstance(queue_config, dict):
            _QUEUES[queue_name] = QueueConfiguration(**queue_config)
        else:
            raise ImproperlyConfigured(f"Queue {queue_name} configuration should be a QueueConfiguration or dict")

    user_settings = getattr(settings, "SCHEDULER_CONFIG", {})
    if isinstance(user_settings, SchedulerConfiguration):
        return
    if not isinstance(user_settings, dict):
        raise ImproperlyConfigured("SCHEDULER_CONFIG should be a SchedulerConfiguration or dict")
    for k in user_settings:
        if k not in SCHEDULER_CONFIG.__annotations__:
            raise ImproperlyConfigured(f"Unknown setting {k} in SCHEDULER_CONFIG")
        setattr(SCHEDULER_CONFIG, k, getattr(user_settings, k, None))


conf_settings()


def get_queue_names() -> List[str]:
    return list(_QUEUES.keys())


def get_queue_configuration(queue_name: str) -> QueueConfiguration:
    if queue_name not in _QUEUES:
        raise QueueNotFoundError(f"Queue {queue_name} not found, queues={_QUEUES.keys()}")
    return _QUEUES[queue_name]
