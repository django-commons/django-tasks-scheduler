# This is a helper module to obfuscate types used by different broker implementations.
from collections import namedtuple
from typing import Any, Callable, TypeVar, Union
from typing import Dict, Tuple

import redis

try:
    import valkey
except ImportError:
    valkey = redis
    valkey.Valkey = redis.Redis
    valkey.StrictValkey = redis.StrictRedis

from scheduler._config_types import Broker

ConnectionErrorTypes = (redis.ConnectionError, valkey.ConnectionError)
ResponseErrorTypes = (redis.ResponseError, valkey.ResponseError)
TimeoutErrorType = Union[redis.TimeoutError, valkey.TimeoutError]
WatchErrorType = Union[redis.WatchError, valkey.WatchError]
ConnectionType = Union[redis.Redis, valkey.Valkey]
PipelineType = Union[redis.client.Pipeline, valkey.client.Pipeline]
SentinelType = Union[redis.sentinel.Sentinel, valkey.sentinel.Sentinel]
FunctionReferenceType = TypeVar("FunctionReferenceType", str, Callable[..., Any])

BrokerMetaDataType = namedtuple("BrokerMetaDataType", ["connection_type", "sentinel_type", "ssl_prefix"])

BrokerMetaData: Dict[Tuple[Broker, bool], BrokerMetaDataType] = {
    # Map of (Broker, Strict flag) => Connection Class, Sentinel Class, SSL Connection Prefix
    (Broker.REDIS, False): BrokerMetaDataType(redis.Redis, redis.sentinel.Sentinel, "rediss"),
    (Broker.VALKEY, False): BrokerMetaDataType(valkey.Valkey, valkey.sentinel.Sentinel, "valkeys"),
    (Broker.REDIS, True): BrokerMetaDataType(redis.StrictRedis, redis.sentinel.Sentinel, "rediss"),
    (Broker.VALKEY, True): BrokerMetaDataType(valkey.StrictValkey, valkey.sentinel.Sentinel, "valkeys"),
}

MODEL_NAMES = ["Task", ]
TASK_TYPES = ["OnceTaskType", "RepeatableTaskType", "CronTaskType"]


def is_pipeline(conn: ConnectionType) -> bool:
    return isinstance(conn, redis.client.Pipeline) or isinstance(conn, valkey.client.Pipeline)
