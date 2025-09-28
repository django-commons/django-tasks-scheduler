import dataclasses
from collections.abc import Sequence
from typing import ClassVar, Optional, List, Tuple, Any

from scheduler.helpers.utils import current_timestamp
from scheduler.redis_models.base import as_str, BaseModel
from scheduler.settings import logger
from scheduler.types import ConnectionType, Self


class DequeueTimeout(Exception):
    pass


@dataclasses.dataclass(slots=True, kw_only=True)
class ZSetModel(BaseModel):
    def cleanup(self, connection: ConnectionType, timestamp: Optional[float] = None) -> None:
        """Remove expired jobs from the registry."""
        score = timestamp or current_timestamp()
        connection.zremrangebyscore(self._key, 0, score)

    def count(self, connection: ConnectionType) -> int:
        """Returns the number of jobs in this registry"""
        self.cleanup(connection=connection)
        return connection.zcard(self._key)

    def add(self, connection: ConnectionType, job_name: str, score: float, update_existing_only: bool = False) -> int:
        logger.debug(f"[registry {self._key}] Adding {job_name} / {score}")
        return connection.zadd(self._key, {job_name: float(score)}, xx=update_existing_only)

    def delete(self, connection: ConnectionType, job_name: str) -> None:
        logger.debug(f"[registry {self._key}] Deleting {job_name}")
        connection.zrem(self._key, job_name)

    def exists(self, connection: ConnectionType, job_name: str) -> bool:
        return connection.zrank(self._key, job_name) is not None


class JobNamesRegistry(ZSetModel):
    _element_key_template: ClassVar[str] = ":registry:{}"

    def __init__(self, name: str) -> None:
        super().__init__(name=name)

    def all(self, connection: ConnectionType, start: int = 0, end: int = -1) -> List[str]:
        """Returns a list of all job names.

        :param connection: Broker connection
        :param start: Start score/timestamp, default to 0.
        :param end: End score/timestamp, default to -1 (i.e., no max score).
        :returns: Returns a list of all job names with timestamp from start to end
        """
        self.cleanup(connection)
        res = [as_str(job_name) for job_name in connection.zrange(self._key, start, end)]
        logger.debug(f"Getting jobs for registry {self.key}: {len(res)} found.")
        return res

    def all_with_timestamps(self, connection: ConnectionType, start: int = 0, end: int = -1) -> List[Tuple[str, float]]:
        """Returns a list of all job names with their timestamps.

        :param connection: Broker connection
        :param start: Start score/timestamp, default to 0.
        :param end: End score/timestamp, default to -1 (i.e., no max score).
        :returns: Returns a list of all job names with timestamp from start to end
        """
        self.cleanup(connection)
        res = connection.zrange(self._key, start, end, withscores=True)
        logger.debug(f"Getting jobs for registry {self._key}: {len(res)} found.")
        return [(as_str(job_name), timestamp) for job_name, timestamp in res]

    def get_first(self, connection: ConnectionType) -> Optional[str]:
        """Returns the first job in the registry."""
        self.cleanup(connection)
        first_job = connection.zrange(self._key, 0, 0)
        return first_job[0].decode() if first_job else None

    def get_last_timestamp(self, connection: ConnectionType) -> Optional[int]:
        """Returns the latest timestamp in the registry."""
        self.cleanup(connection)
        last_timestamp = connection.zrange(self._key, -1, -1, withscores=True)
        return int(last_timestamp[0][1]) if last_timestamp else None

    @property
    def key(self) -> str:
        return self._key

    @classmethod
    def pop(
        cls, connection: ConnectionType, registries: Sequence[Self], timeout: Optional[int]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Helper method to abstract away from some Redis API details

        :param connection: Broker connection
        :param registries: List of registries to pop from
        :param timeout: Timeout in seconds
        :raises ValueError: If timeout of 0 was passed
        :raises DequeueTimeout: BLPOP Timeout
        :returns: Tuple of registry key and job name
        """
        if timeout == 0:
            raise ValueError("Indefinite timeout not supported. Please pick a timeout value > 0")
        registry_keys = [r.key for r in registries]
        if timeout is not None:  # blocking variant
            colored_registries = ",".join(map(str, [str(registry) for registry in registry_keys]))
            logger.debug(f"Starting BZMPOP operation for queues {colored_registries} with timeout of {timeout}")
            result = connection.bzpopmin(registry_keys, timeout)
            if not result:
                logger.debug(f"BZMPOP timeout, no jobs found on queues {colored_registries}")
                raise DequeueTimeout(timeout, registry_keys)
            registry_key, job_name, timestamp = result
            return as_str(registry_key), as_str(job_name)
        else:  # non-blocking variant
            for registry_key in registry_keys:
                results: List[Any] = connection.zpopmin(registry_key)
                if results:
                    job_name, timestamp = results[0]
                    return as_str(registry_key), as_str(job_name)
            return None, None
