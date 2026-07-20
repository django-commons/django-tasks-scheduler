from typing import Any, Dict, Set

from scheduler.redis_models.worker import WorkerModel
from scheduler.settings import SCHEDULER_CONFIG, get_queue_names, get_queue_configuration, logger
from scheduler.types import ConnectionErrorTypes, BrokerMetaData, Broker, ConnectionType, QueueConfiguration
from .queue_logic import Queue


def _fail_fast_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Returns a copy of `kwargs` with short-timeout, no-retry connection settings applied, unless the
    user already configured them explicitly (see `SchedulerConfiguration.FAIL_FAST_QUEUE_PROBING`)."""
    kwargs = dict(kwargs)
    kwargs.setdefault("socket_connect_timeout", SCHEDULER_CONFIG.QUEUE_PROBE_SOCKET_CONNECT_TIMEOUT)
    kwargs.setdefault("retry", None)
    return kwargs


def _get_connection(
    config: QueueConfiguration, use_strict_broker: bool = False, fail_fast: bool = False
) -> ConnectionType:
    """Returns a Broker connection to use based on parameters in SCHEDULER_QUEUES.

    `fail_fast` is used by admin views that probe every configured queue for read-only operations
    (e.g. locating a job across queues, queue/worker statistics), so that a single unreachable queue
    cannot stall the request for several seconds (redis-py >= 8 retries connection errors with backoff
    by default). See `SchedulerConfiguration.FAIL_FAST_QUEUE_PROBING` to opt out.
    """
    if SCHEDULER_CONFIG.BROKER == Broker.FAKEREDIS:
        import fakeredis

        broker_cls = fakeredis.FakeRedis if not use_strict_broker else fakeredis.FakeStrictRedis
    else:
        broker_cls = BrokerMetaData[(SCHEDULER_CONFIG.BROKER, use_strict_broker)].connection_type

    apply_fail_fast = fail_fast and SCHEDULER_CONFIG.FAIL_FAST_QUEUE_PROBING
    connection_kwargs = config.CONNECTION_KWARGS or {}
    if apply_fail_fast:
        connection_kwargs = _fail_fast_kwargs(connection_kwargs)

    if config.URL:
        return broker_cls.from_url(config.URL, db=config.DB, **connection_kwargs)
    if config.UNIX_SOCKET_PATH:
        return broker_cls(unix_socket_path=config.UNIX_SOCKET_PATH, db=config.DB)

    if config.SENTINELS:
        full_connection_kwargs = {
            "db": config.DB,
            "password": config.PASSWORD,
            "username": config.USERNAME,
        }
        full_connection_kwargs.update(connection_kwargs)
        sentinel_kwargs = config.SENTINEL_KWARGS or {}
        if apply_fail_fast:
            sentinel_kwargs = _fail_fast_kwargs(sentinel_kwargs)
        SentinelClass = BrokerMetaData[(SCHEDULER_CONFIG.BROKER, use_strict_broker)].sentinel_type
        sentinel = SentinelClass(config.SENTINELS, sentinel_kwargs=sentinel_kwargs, **full_connection_kwargs)
        return sentinel.master_for(  # type: ignore
            service_name=config.MASTER_NAME,
            redis_class=broker_cls,
        )

    return broker_cls(
        host=config.HOST,
        port=config.PORT,
        db=config.DB,
        username=config.USERNAME,
        password=config.PASSWORD,
        **connection_kwargs,
    )


def get_queue_connection(queue_name: str, fail_fast: bool = False) -> ConnectionType:
    queue_settings = get_queue_configuration(queue_name)
    connection = _get_connection(queue_settings, fail_fast=fail_fast)
    return connection


def get_queue(name: str = "default", fail_fast: bool = False) -> Queue:
    """Returns an DjangoQueue using parameters defined in `SCHEDULER_QUEUES`"""
    queue_settings = get_queue_configuration(name)
    is_async = queue_settings.ASYNC
    connection = _get_connection(queue_settings, fail_fast=fail_fast)
    return Queue(name=name, connection=connection, is_async=is_async)


def get_all_workers() -> Set[WorkerModel]:
    queue_names = get_queue_names()

    workers_set: Set[WorkerModel] = set()
    for queue_name in queue_names:
        connection = _get_connection(get_queue_configuration(queue_name), fail_fast=True)
        try:
            curr_workers: Set[WorkerModel] = set(WorkerModel.all(connection=connection))
            workers_set.update(curr_workers)
        except ConnectionErrorTypes as e:
            logger.error(f"Could not connect for queue {queue_name}: {e}")
    return workers_set
