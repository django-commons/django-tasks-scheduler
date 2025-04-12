import dataclasses
from datetime import datetime
from enum import Enum
from typing import List, Optional, ClassVar, Any, Generator

from scheduler.helpers.utils import utcnow
from scheduler.redis_models.base import HashModel, MAX_KEYS
from scheduler.settings import logger
from scheduler.types import ConnectionType, Self

DEFAULT_WORKER_TTL = 420


class WorkerStatus(str, Enum):
    CREATED = "created"
    STARTING = "starting"
    STARTED = "started"
    SUSPENDED = "suspended"
    BUSY = "busy"
    IDLE = "idle"


@dataclasses.dataclass(slots=True, kw_only=True)
class WorkerModel(HashModel):
    name: str
    queue_names: List[str]
    pid: int
    hostname: str
    ip_address: str
    version: str
    python_version: str
    state: WorkerStatus
    job_execution_process_pid: int = 0
    successful_job_count: int = 0
    failed_job_count: int = 0
    completed_jobs: int = 0
    birth: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    is_suspended: bool = False
    current_job_name: Optional[str] = None
    stopped_job_name: Optional[str] = None
    total_working_time_ms: float = 0.0
    current_job_working_time: float = 0
    last_cleaned_at: Optional[datetime] = None
    shutdown_requested_date: Optional[datetime] = None
    has_scheduler: bool = False
    death: Optional[datetime] = None

    _list_key: ClassVar[str] = ":workers:ALL:"
    _children_key_template: ClassVar[str] = ":queue-workers:{}:"
    _element_key_template: ClassVar[str] = ":workers:{}"

    def save(self, connection: ConnectionType) -> None:
        pipeline = connection.pipeline()
        super(WorkerModel, self).save(pipeline)
        for queue_name in self.queue_names:
            pipeline.sadd(self._children_key_template.format(queue_name), self.name)
        pipeline.expire(self._key, DEFAULT_WORKER_TTL + 60)
        pipeline.execute()

    def delete(self, connection: ConnectionType) -> None:
        logger.debug(f"Deleting worker {self.name}")
        pipeline = connection.pipeline()
        now = utcnow()
        self.death = now
        pipeline.hset(self._key, "death", now.isoformat())
        pipeline.expire(self._key, 60)
        pipeline.srem(self._list_key, self.name)
        for queue_name in self.queue_names:
            pipeline.srem(self._children_key_template.format(queue_name), self.name)
        pipeline.execute()

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, self.__class__):
            raise TypeError("Cannot compare workers to other types (of workers)")
        return self._key == other._key

    def __hash__(self):
        """The hash does not take the database/connection into account"""
        return hash((self._key, ",".join(self.queue_names)))

    def set_current_job_working_time(self, job_execution_time: int, connection: ConnectionType) -> None:
        self.set_field("current_job_working_time", job_execution_time, connection=connection)

    def heartbeat(self, connection: ConnectionType, timeout: Optional[int] = None) -> None:
        timeout = timeout or DEFAULT_WORKER_TTL + 60
        connection.expire(self._key, timeout)
        now = utcnow()
        self.set_field("last_heartbeat", now, connection=connection)
        logger.debug(f"Next heartbeat for worker {self._key} should arrive in {timeout} seconds.")

    @classmethod
    def cleanup(cls, connection: ConnectionType, queue_name: Optional[str] = None):
        worker_names = cls.all_names(connection, queue_name)
        worker_keys = [cls.key_for(worker_name) for worker_name in worker_names]
        with connection.pipeline() as pipeline:
            for worker_key in worker_keys:
                pipeline.exists(worker_key)
            worker_exist = pipeline.execute()
            invalid_workers = list()
            for i, worker_name in enumerate(worker_names):
                if not worker_exist[i]:
                    invalid_workers.append(worker_name)
            if len(invalid_workers) == 0:
                return
            for invalid_subset in _split_list(invalid_workers, MAX_KEYS):
                pipeline.srem(cls._list_key, *invalid_subset)
                if queue_name:
                    pipeline.srem(cls._children_key_template.format(queue_name), *invalid_subset)
                pipeline.execute()


def _split_list(a_list: List[str], segment_size: int) -> Generator[list[str], Any, None]:
    """Splits a list into multiple smaller lists having size `segment_size`

    :param a_list: The list to split
    :param segment_size: The segment size to split into
    :returns: The list split into smaller lists
    """
    for i in range(0, len(a_list), segment_size):
        yield a_list[i : i + segment_size]
