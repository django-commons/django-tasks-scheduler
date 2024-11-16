from typing import Union, Dict, Tuple, Type

import redis

try:
    import valkey
except ImportError:
    valkey = redis
    valkey.Valkey = redis.Redis
    valkey.StrictValkey = redis.StrictRedis

from scheduler.settings import Broker

ConnectionErrorTypes = (redis.ConnectionError, valkey.ConnectionError)
ResponseErrorTypes = (redis.ResponseError, valkey.ResponseError)
ConnectionType = Union[redis.Redis, valkey.Valkey]
PipelineType = Union[redis.client.Pipeline, valkey.client.Pipeline]
SentinelType = Union[redis.sentinel.Sentinel, valkey.sentinel.Sentinel]

BrokerMetaData: Dict[Tuple[Broker, bool], Tuple[Type[ConnectionType], Type[SentinelType], str]] = {
    # Map of (Broker, Strict flag) => Connection Class, Sentinel Class, SSL Connection Prefix
    (Broker.REDIS, False): (redis.Redis, redis.sentinel.Sentinel, "rediss"),
    (Broker.VALKEY, False): (valkey.Valkey, valkey.sentinel.Sentinel, "valkeys"),
    (Broker.REDIS, True): (redis.StrictRedis, redis.sentinel.Sentinel, "rediss"),
    (Broker.VALKEY, True): (valkey.StrictValkey, valkey.sentinel.Sentinel, "valkeys"),
}
