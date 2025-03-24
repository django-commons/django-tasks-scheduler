from typing import List, Set

from scheduler.broker_types import ConnectionErrorTypes, BrokerMetaData
from scheduler.redis_models.worker import WorkerModel
from scheduler.settings import (
    SCHEDULER_CONFIG,
    get_queue_names,
    get_queue_configuration,
    QueueConfiguration,
    logger,
    Broker,
)
from .queue_logic import Queue


class QueueConnectionDiscrepancyError(Exception):
    pass


_BAD_QUEUE_CONFIGURATION = set()


def _get_connection(config: QueueConfiguration, use_strict_broker=False):
    """Returns a Broker connection to use based on parameters in SCHEDULER_QUEUES"""
    """
        Returns a redis connection from a connection config
        """
    if SCHEDULER_CONFIG.BROKER == Broker.FAKEREDIS:
        import fakeredis

        broker_cls = fakeredis.FakeRedis if not use_strict_broker else fakeredis.FakeStrictRedis
    else:
        broker_cls = BrokerMetaData[(SCHEDULER_CONFIG.BROKER, use_strict_broker)].connection_type
    if config.URL:
        return broker_cls.from_url(config.URL, db=config.DB, **(config.CONNECTION_KWARGS or {}))
    if config.UNIX_SOCKET_PATH:
        return broker_cls(unix_socket_path=config.UNIX_SOCKET_PATH, db=config.DB)

    if config.SENTINELS:
        connection_kwargs = {
            "db": config.DB,
            "password": config.PASSWORD,
            "username": config.USERNAME,
        }
        connection_kwargs.update(config.CONNECTION_KWARGS or {})
        sentinel_kwargs = config.SENTINEL_KWARGS or {}
        SentinelClass = BrokerMetaData[(SCHEDULER_CONFIG.BROKER, use_strict_broker)].sentinel_type
        sentinel = SentinelClass(config.SENTINELS, sentinel_kwargs=sentinel_kwargs, **connection_kwargs)
        return sentinel.master_for(
            service_name=config.MASTER_NAME,
            redis_class=broker_cls,
        )

    return broker_cls(
        host=config.HOST,
        port=config.PORT,
        db=config.DB,
        username=config.USERNAME,
        password=config.PASSWORD,
        **(config.CONNECTION_KWARGS or {}),
    )


def get_queue(name="default") -> Queue:
    """Returns an DjangoQueue using parameters defined in `SCHEDULER_QUEUES`"""
    queue_settings = get_queue_configuration(name)
    is_async = queue_settings.ASYNC
    connection = _get_connection(queue_settings)
    return Queue(name=name, connection=connection, is_async=is_async)


def get_all_workers() -> Set[WorkerModel]:
    queue_names = get_queue_names()

    workers_set: Set[WorkerModel] = set()
    for queue_name in queue_names:
        if queue_name in _BAD_QUEUE_CONFIGURATION:
            continue
        connection = _get_connection(get_queue_configuration(queue_name))
        try:
            curr_workers: Set[WorkerModel] = set(WorkerModel.all(connection=connection))
            workers_set.update(curr_workers)
        except ConnectionErrorTypes as e:
            logger.error(f"Could not connect for queue {queue_name}: {e}")
            _BAD_QUEUE_CONFIGURATION.add(queue_name)
    return workers_set


def get_queues(*queue_names: str) -> List[Queue]:
    """Return queue instances from specified queue names. All instances must use the same Broker configuration."""

    queue_config = get_queue_configuration(queue_names[0])
    queues = [get_queue(queue_names[0])]
    # perform consistency checks while building return list
    for queue_name in queue_names[1:]:
        curr_queue_config = get_queue_configuration(queue_name)
        if not queue_config.same_connection_params(curr_queue_config):
            raise QueueConnectionDiscrepancyError(
                f'Queues must have the same broker connection. "{queue_name}" and "{queue_names[0]}" have different connection settings'
            )
        queue = get_queue(queue_name)
        queues.append(queue)

    return queues
