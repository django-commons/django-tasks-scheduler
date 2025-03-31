import logging
from typing import List, Dict

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from scheduler.settings_types import SchedulerConfiguration, Broker, QueueConfiguration

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
    if app_queues is None:
        logger.warning("Configuration using RQ_QUEUES is deprecated. Use SCHEDULER_QUEUES instead")
        app_queues = getattr(settings, "RQ_QUEUES", None)
    if app_queues is None:
        raise ImproperlyConfigured("You have to define SCHEDULER_QUEUES in settings.py")

    for queue_name, queue_config in app_queues.items():
        if isinstance(queue_config, QueueConfiguration):
            _QUEUES[queue_name] = queue_config
        else:
            _QUEUES[queue_name] = QueueConfiguration(**queue_config)

    user_settings = getattr(settings, "SCHEDULER_CONFIG", {})
    if isinstance(user_settings, SchedulerConfiguration):
        return
    if "FAKEREDIS" in user_settings:
        logger.warning("Configuration using FAKEREDIS is deprecated. Use BROKER='fakeredis' instead")
        user_settings["BROKER"] = Broker.FAKEREDIS if user_settings["FAKEREDIS"] else Broker.REDIS
        user_settings.pop("FAKEREDIS")
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
