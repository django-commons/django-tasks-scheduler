from typing import List, Dict, Set

from .broker_types import ConnectionErrorTypes, BrokerMetaData
from .rq_classes import JobExecution, DjangoQueue, DjangoWorker
from .settings import SCHEDULER_CONFIG
from .settings import logger, Broker

_CONNECTION_PARAMS = {
    "URL",
    "DB",
    "USE_REDIS_CACHE",
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


class QueueNotFoundError(Exception):
    pass


def _get_broker_connection(config, use_strict_broker=False):
    """
    Returns a redis connection from a connection config
    """
    if SCHEDULER_CONFIG.BROKER == Broker.FAKEREDIS:
        import fakeredis

        broker_cls = fakeredis.FakeRedis if not use_strict_broker else fakeredis.FakeStrictRedis
    else:
        broker_cls = BrokerMetaData[(SCHEDULER_CONFIG.BROKER, use_strict_broker)].connection_type
    logger.debug(f"Getting connection for {config}")
    if "URL" in config:
        ssl_url_protocol = BrokerMetaData[(SCHEDULER_CONFIG.BROKER, use_strict_broker)].ssl_prefix
        if config.get("SSL") or config.get("URL").startswith(f"{ssl_url_protocol}://"):
            return broker_cls.from_url(
                config["URL"],
                db=config.get("DB"),
                ssl_cert_reqs=config.get("SSL_CERT_REQS", "required"),
            )
        else:
            return broker_cls.from_url(
                config["URL"],
                db=config.get("DB"),
            )
    if "UNIX_SOCKET_PATH" in config:
        return broker_cls(unix_socket_path=config["UNIX_SOCKET_PATH"], db=config["DB"])

    if "SENTINELS" in config:
        connection_kwargs = {
            "db": config.get("DB"),
            "password": config.get("PASSWORD"),
            "username": config.get("USERNAME"),
            "socket_timeout": config.get("SOCKET_TIMEOUT"),
        }
        connection_kwargs.update(config.get("CONNECTION_KWARGS", {}))
        sentinel_kwargs = config.get("SENTINEL_KWARGS", {})
        SentinelClass = BrokerMetaData[(SCHEDULER_CONFIG.BROKER, use_strict_broker)].sentinel_type
        sentinel = SentinelClass(config["SENTINELS"], sentinel_kwargs=sentinel_kwargs, **connection_kwargs)
        return sentinel.master_for(
            service_name=config["MASTER_NAME"],
            redis_class=broker_cls,
        )

    return broker_cls(
        host=config["HOST"],
        port=config["PORT"],
        db=config.get("DB", 0),
        username=config.get("USERNAME", None),
        password=config.get("PASSWORD"),
        ssl=config.get("SSL", False),
        ssl_cert_reqs=config.get("SSL_CERT_REQS", "required"),
        **config.get("CLIENT_KWARGS", {}),
    )


def get_connection(queue_settings, use_strict_redis=False):
    """Returns a Broker connection to use based on parameters in SCHEDULER_QUEUES"""
    return _get_broker_connection(queue_settings, use_strict_redis)


def get_queue(
    name="default", default_timeout=None, is_async=None, autocommit=None, connection=None, **kwargs
) -> DjangoQueue:
    """Returns an DjangoQueue using parameters defined in `SCHEDULER_QUEUES`"""
    from .settings import QUEUES

    if name not in QUEUES:
        raise QueueNotFoundError(f"Queue {name} not found, queues={QUEUES.keys()}")
    queue_settings = QUEUES[name]
    if is_async is None:
        is_async = queue_settings.get("ASYNC", True)

    if default_timeout is None:
        default_timeout = queue_settings.get("DEFAULT_TIMEOUT")
    if connection is None:
        connection = get_connection(queue_settings)
    return DjangoQueue(
        name, default_timeout=default_timeout, connection=connection, is_async=is_async, autocommit=autocommit, **kwargs
    )


def get_all_workers() -> Set[DjangoWorker]:
    from .settings import QUEUES

    workers_set: Set[DjangoWorker] = set()
    for queue_name in QUEUES:
        connection = get_connection(QUEUES[queue_name])
        try:
            curr_workers: Set[DjangoWorker] = set(DjangoWorker.all(connection=connection))
            workers_set.update(curr_workers)
        except ConnectionErrorTypes as e:
            logger.error(f"Could not connect for queue {queue_name}: {e}")
    return workers_set


def _queues_share_connection_params(q1_params: Dict, q2_params: Dict):
    """Check that both queues share the same connection parameters"""
    return all(
        ((p not in q1_params and p not in q2_params) or (q1_params.get(p, None) == q2_params.get(p, None)))
        for p in _CONNECTION_PARAMS
    )


def get_queues(*queue_names, **kwargs) -> List[DjangoQueue]:
    """Return queue instances from specified queue names.
    All instances must use the same Redis connection.
    """
    from .settings import QUEUES

    kwargs["job_class"] = JobExecution
    queue_params = QUEUES[queue_names[0]]
    queues = [get_queue(queue_names[0], **kwargs)]
    # perform consistency checks while building return list
    for name in queue_names[1:]:
        if not _queues_share_connection_params(queue_params, QUEUES[name]):
            raise ValueError(
                f'Queues must have the same broker connection. "{name}" and'
                f' "{queue_names[0]}" have different connections'
            )
        queue = get_queue(name, **kwargs)
        queues.append(queue)

    return queues
