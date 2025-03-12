import dataclasses
from collections.abc import Sequence
from typing import ClassVar, Optional, List, Self, Tuple, Any

from scheduler.broker_types import ConnectionType
from scheduler.redis_models.base import as_str, BaseModel
from scheduler.settings import logger
from scheduler.helpers.utils import current_timestamp


class DequeueTimeout(Exception):
    pass


@dataclasses.dataclass(slots=True, kw_only=True)
class ZSetModel(BaseModel):

    def cleanup(self, connection: ConnectionType, timestamp: Optional[float] = None) -> None:
        """Remove expired jobs from registry."""
        score = timestamp or current_timestamp()
        connection.zremrangebyscore(self._key, 0, score)

    def count(self, connection: ConnectionType) -> int:
        """Returns the number of jobs in this registry"""
        self.cleanup(connection=connection)
        return connection.zcard(self._key)

    def add(
            self,
            connection: ConnectionType,
            member: str,
            score: float,
            update_existing_only: bool = False) -> int:
        return connection.zadd(self._key, {member: float(score)}, xx=update_existing_only)

    def delete(self, connection: ConnectionType, job_name: str) -> None:
        connection.zrem(self._key, job_name)


class JobNamesRegistry(ZSetModel):
    _element_key_template: ClassVar[str] = ":registry:{}"

    def __init__(self, connection: ConnectionType, name: str) -> None:
        super().__init__(name=name)
        self.connection = connection

    def __len__(self) -> int:
        return self.count(self.connection)

    def all(self, start: int = 0, end: int = -1) -> List[str]:
        """Returns list of all job ids.

        :param start: Start score/timestamp, default to 0.
        :param end: End score/timestamp, default to -1 (i.e., no max score).
        :returns: Returns list of all job ids with timestamp from start to end
        """
        self.cleanup(self.connection)
        res = [as_str(job_id) for job_id in self.connection.zrange(self._key, start, end)]
        logger.debug(f"Getting jobs for registry {self._key}: {len(res)} found.")
        return res

    def all_with_timestamps(self, start: int = 0, end: int = -1) -> List[tuple[str, float]]:
        """Returns list of all job ids with their timestamps.

        :param start: Start score/timestamp, default to 0.
        :param end: End score/timestamp, default to -1 (i.e., no max score).
        :returns: Returns list of all job ids with timestamp from start to end
        """
        self.cleanup(self.connection)
        res = self.connection.zrange(self._key, start, end, withscores=True)
        logger.debug(f"Getting jobs for registry {self._key}: {len(res)} found.")
        return [(as_str(job_id), timestamp) for job_id, timestamp in res]

    def get_first(self) -> Optional[str]:
        """Returns the first job in the registry."""
        self.cleanup(self.connection)
        first_job = self.connection.zrange(self._key, 0, 0)
        return first_job[0].decode() if first_job else None

    def get_last_timestamp(self) -> Optional[float]:
        """Returns the last timestamp in the registry."""
        self.cleanup(self.connection)
        last_timestamp = self.connection.zrange(self._key, -1, -1, withscores=True)
        return last_timestamp[0][1] if last_timestamp else None

    @property
    def key(self) -> str:
        return self._key

    @classmethod
    def pop(
            cls, connection: ConnectionType, registries: Sequence[Self], timeout: Optional[int]
    ) -> Tuple[Optional[str], Optional[Tuple[str, float]]]:
        """Helper method to abstract away from some Redis API details

        :param connection: Broker connection
        :param registries: List of registries to pop from
        :param timeout: Timeout in seconds
        :raises ValueError: If timeout of 0 was passed
        :raises DequeueTimeout: BLPOP Timeout
        :returns: Tuple of registry key and job id
        """
        if timeout == 0:
            raise ValueError("Indefinite timeout not supported. Please pick a timeout value > 0")
        registry_keys = [r.key for r in registries]
        if timeout is not None:  # blocking variant
            colored_registries = ", ".join(map(str, [str(registry) for registry in registry_keys]))
            logger.debug(f"Starting BZMPOP operation for queues {colored_registries} with timeout of {timeout}")
            result = connection.bzpopmin(registry_keys, timeout)
            if not result:
                logger.debug(f"BZMPOP timeout, no jobs found on queues {colored_registries}")
                raise DequeueTimeout(timeout, registry_keys)
            registry_key, job_id = result
            return registry_key, job_id
        else:  # non-blocking variant
            for registry_key in registry_keys:
                results: List[Any] = connection.zpopmin(registry_key)
                if results:
                    job_id, timestamp = results[0]
                    return registry_key, job_id
            return None, None
