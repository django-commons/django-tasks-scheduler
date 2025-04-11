import sys
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Optional, List, Tuple, Any, Type

from scheduler.timeouts import BaseDeathPenalty, UnixSignalDeathPenalty

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Broker(Enum):
    REDIS = "redis"
    FAKEREDIS = "fakeredis"
    VALKEY = "valkey"


def _token_validation(token: str) -> bool:
    return False


@dataclass(slots=True, kw_only=True)
class SchedulerConfiguration:
    """Configuration for django-tasks-scheduler"""

    EXECUTIONS_IN_PAGE: int = 20
    SCHEDULER_INTERVAL: int = 10
    BROKER: Broker = Broker.REDIS
    TOKEN_VALIDATION_METHOD: Callable[[str], bool] = _token_validation
    CALLBACK_TIMEOUT: int = 60  # Callback timeout in seconds (success/failure/stopped)
    # Default values, can be override per task
    DEFAULT_SUCCESS_TTL: int = 10 * 60  # Time To Live (TTL) in seconds to keep successful job results
    DEFAULT_FAILURE_TTL: int = 365 * 24 * 60 * 60  # Time To Live (TTL) in seconds to keep job failure information
    DEFAULT_JOB_TTL: int = 10 * 60  # Time To Live (TTL) in seconds to keep job information
    DEFAULT_JOB_TIMEOUT: int = 5 * 60  # timeout (seconds) for a job
    # General configuration values
    DEFAULT_WORKER_TTL: int = 10 * 60  # Time To Live (TTL) in seconds to keep worker information after last heartbeat
    DEFAULT_MAINTENANCE_TASK_INTERVAL: int = 10 * 60  # The interval to run maintenance tasks in seconds. 10 minutes.
    DEFAULT_JOB_MONITORING_INTERVAL: int = 30  # The interval to monitor jobs in seconds.
    SCHEDULER_FALLBACK_PERIOD_SECS: int = 120  # Period (secs) to wait before requiring to reacquire locks
    DEATH_PENALTY_CLASS: Type["BaseDeathPenalty"] = UnixSignalDeathPenalty


@dataclass(slots=True, frozen=True, kw_only=True)
class QueueConfiguration:
    __CONNECTION_FIELDS__ = {
        "URL",
        "DB",
        "UNIX_SOCKET_PATH",
        "HOST",
        "PORT",
        "PASSWORD",
        "SENTINELS",
        "MASTER_NAME",
        "CONNECTION_KWARGS",
    }
    DB: Optional[int] = None
    # Redis connection parameters, either UNIX_SOCKET_PATH/URL/separate params (HOST, PORT, PASSWORD) should be provided
    UNIX_SOCKET_PATH: Optional[str] = None
    URL: Optional[str] = None
    HOST: Optional[str] = None
    PORT: Optional[int] = None
    USERNAME: Optional[str] = None
    PASSWORD: Optional[str] = None

    ASYNC: Optional[bool] = True

    SENTINELS: Optional[List[Tuple[str, int]]] = None
    SENTINEL_KWARGS: Optional[Dict[str, str]] = None
    MASTER_NAME: Optional[str] = None
    CONNECTION_KWARGS: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if not any((self.URL, self.UNIX_SOCKET_PATH, self.HOST, self.SENTINELS)):
            raise ValueError(f"At least one of URL, UNIX_SOCKET_PATH, HOST must be provided: {self}")
        if sum((self.URL is not None, self.UNIX_SOCKET_PATH is not None, self.HOST is not None)) > 1:
            raise ValueError(f"Only one of URL, UNIX_SOCKET_PATH, HOST should be provided: {self}")
        if self.HOST is not None and (self.PORT is None or self.DB is None):
            raise ValueError(f"HOST requires PORT and DB: {self}")

    def same_connection_params(self, other: Self) -> bool:
        for field in self.__CONNECTION_FIELDS__:
            if getattr(self, field) != getattr(other, field):
                return False
        return True
