import logging
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

logger = logging.getLogger(__package__)

QUEUES = dict()


class Broker(Enum):
    REDIS = "redis"
    FAKEREDIS = "fakeredis"
    VALKEY = "valkey"


@dataclass
class SchedulerConfig:
    EXECUTIONS_IN_PAGE: int
    DEFAULT_RESULT_TTL: int
    DEFAULT_TIMEOUT: int
    SCHEDULER_INTERVAL: int
    BROKER: Broker
    TOKEN_VALIDATION_METHOD: Callable[[str], bool]


def _token_validation(token: str) -> bool:
    return False


SCHEDULER_CONFIG: SchedulerConfig = SchedulerConfig(
    EXECUTIONS_IN_PAGE=20,
    DEFAULT_RESULT_TTL=600,
    DEFAULT_TIMEOUT=300,
    SCHEDULER_INTERVAL=10,
    BROKER=Broker.REDIS,
    TOKEN_VALIDATION_METHOD=_token_validation,
)


def conf_settings():
    global QUEUES
    global SCHEDULER_CONFIG

    QUEUES = getattr(settings, "SCHEDULER_QUEUES", None)
    if QUEUES is None:
        logger.warning("Configuration using RQ_QUEUES is deprecated. Use SCHEDULER_QUEUES instead")
        QUEUES = getattr(settings, "RQ_QUEUES", None)
    if QUEUES is None:
        raise ImproperlyConfigured("You have to define SCHEDULER_QUEUES in settings.py")

    user_settings = getattr(settings, "SCHEDULER_CONFIG", {})
    if "FAKEREDIS" in user_settings:
        logger.warning("Configuration using FAKEREDIS is deprecated. Use BROKER='fakeredis' instead")
        user_settings["BROKER"] = Broker.FAKEREDIS if user_settings["FAKEREDIS"] else Broker.REDIS
        user_settings.pop("FAKEREDIS")
    for k in user_settings:
        if k not in SCHEDULER_CONFIG.__annotations__:
            raise ImproperlyConfigured(f"Unknown setting {k} in SCHEDULER_CONFIG")
        setattr(SCHEDULER_CONFIG, k, user_settings[k])


conf_settings()
