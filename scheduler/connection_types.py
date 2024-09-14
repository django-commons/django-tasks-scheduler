from typing import Union, Dict, Tuple, Type

import redis
import valkey

from scheduler.settings import Broker

ConnectionErrorType = Union[redis.ConnectionError, valkey.ConnectionError]
ConnectionType = Union[redis.Redis, valkey.Valkey]
PipelineType = Union[redis.client.Pipeline, valkey.client.Pipeline]
RedisSentinel = redis.sentinel.Sentinel

BrokerConnectionClass: Dict[Tuple[Broker, bool], Type] = {
    # Map of (Broker, Strict flag) => Connection Class
    (Broker.REDIS, False): redis.Redis,
    (Broker.VALKEY, False): valkey.Valkey,
    (Broker.REDIS, True): redis.StrictRedis,
    (Broker.VALKEY, True): valkey.StrictValkey,
}
