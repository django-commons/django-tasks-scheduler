from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Optional, List, Tuple, Any, Self, Type

from scheduler.helpers.timeouts import BaseDeathPenalty, UnixSignalDeathPenalty


@dataclass
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
        "SOCKET_TIMEOUT",
        "SSL",
        "CONNECTION_KWARGS",
    }
    DB: Optional[int] = None
    CLIENT_KWARGS: Optional[Dict[str, Any]] = None

    # Redis connection parameters, either UNIX_SOCKET_PATH/URL/separate params (HOST, PORT, PASSWORD) should be provided
    UNIX_SOCKET_PATH: Optional[str] = None

    URL: Optional[str] = None

    HOST: Optional[str] = None
    PORT: Optional[int] = None
    USERNAME: Optional[str] = None
    PASSWORD: Optional[str] = None

    SSL: Optional[bool] = False
    SSL_CERT_REQS: Optional[str] = "required"

    DEFAULT_TIMEOUT: Optional[int] = None
    ASYNC: Optional[bool] = True

    SENTINELS: Optional[List[Tuple[str, int]]] = None
    SENTINEL_KWARGS: Optional[Dict[str, str]] = None
    SOCKET_TIMEOUT: Optional[int] = None
    MASTER_NAME: Optional[str] = None
    CONNECTION_KWARGS: Optional[Dict[str, Any]] = None

    def same_connection_params(self, other: Self) -> bool:
        for field in self.__CONNECTION_FIELDS__:
            if getattr(self, field) != getattr(other, field):
                return False
        return True


class Broker(Enum):
    REDIS = "redis"
    FAKEREDIS = "fakeredis"
    VALKEY = "valkey"


def _token_validation(token: str) -> bool:
    return False


@dataclass
class SchedulerConfig:
    EXECUTIONS_IN_PAGE: int = 20
    SCHEDULER_INTERVAL: int = 10
    BROKER: Broker = Broker.REDIS
    TOKEN_VALIDATION_METHOD: Callable[[str], bool] = _token_validation
    CALLBACK_TIMEOUT = 60  # Callback timeout in seconds (success/failure)
    # Default values, can be override per task
    DEFAULT_RESULT_TTL: int = 500  # Time To Live (TTL) in seconds to keep job results
    DEFAULT_FAILURE_TTL: int = 31536000  # Time To Live (TTL) in seconds to keep job failure information
    DEFAULT_JOB_TIMEOUT: int = 300  # timeout (seconds) for a job)
    # General configuration values
    DEFAULT_WORKER_TTL = 420  # Time To Live (TTL) in seconds to keep worker information after last heartbeat
    DEFAULT_MAINTENANCE_TASK_INTERVAL = 10 * 60  # The interval to run maintenance tasks in seconds. 10 minutes.
    DEFAULT_JOB_MONITORING_INTERVAL = 30  # The interval to monitor jobs in seconds.
    SCHEDULER_FALLBACK_PERIOD_SECS: int = 120  # Period (secs) to wait before requiring to reacquire locks
    DEATH_PENALTY_CLASS : Type[BaseDeathPenalty] = UnixSignalDeathPenalty