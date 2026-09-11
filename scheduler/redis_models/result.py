import dataclasses
from collections.abc import Sequence
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Optional

from scheduler.helpers.utils import utcnow
from scheduler.redis_models.base import StreamModel, decode_dict
from scheduler.settings import logger
from scheduler.types import ConnectionType, Self


class ResultType(Enum):
    SUCCESSFUL = "successful"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclasses.dataclass(slots=True, kw_only=True)
class Result(StreamModel):
    parent: str
    type: ResultType
    worker_name: str
    ttl: int | None = None
    name: str | None = None
    created_at: datetime = dataclasses.field(default_factory=utcnow)
    return_value: Any | None = None
    exc_string: str | None = None

    _list_key: ClassVar[str] = ":job-results:"
    _children_key_template: ClassVar[str] = ":job-results:{}:"
    _element_key_template: ClassVar[str] = ":job-results:{}"

    @classmethod
    def create(
        cls,
        connection: ConnectionType,
        job_name: str,
        worker_name: str | None,
        _type: ResultType,
        ttl: int,
        return_value: Any = None,
        exc_string: str | None = None,
    ) -> Self:
        if worker_name is None:
            logger.warning(f"Job {job_name} has no worker name, will save result with 'unknown_worker'")
            worker_name = "unknown_worker"
        result = cls(
            parent=job_name,
            ttl=ttl,
            type=_type,
            return_value=return_value,
            exc_string=exc_string,
            worker_name=worker_name,
        )
        result.save(connection)
        return result

    def save(self, connection: ConnectionType, ttl: int | None = None) -> bool:
        """Saves the result, expiring the job results stream according to the result ttl."""
        return super(Result, self).save(connection, ttl=self.ttl if ttl is None else ttl)

    @classmethod
    def fetch_latest(cls, connection: ConnectionType, job_name: str) -> Optional["Result"]:
        """Returns the latest result for given job_name.

        :param connection: Broker connection.
        :param job_name: Job name.
        :return: Result instance or None if no result is available.
        """
        return cls.fetch_latest_many(connection, [job_name]).get(job_name)

    @classmethod
    def fetch_latest_many(cls, connection: ConnectionType, job_names: Sequence[str]) -> dict[str, "Result"]:
        """Returns the latest result of each of `job_names` that has one, fetching them all in one round trip."""
        if not job_names:
            return {}
        with connection.pipeline() as pipeline:
            for job_name in job_names:
                pipeline.xrevrange(cls._children_key_template.format(job_name), "+", "-", count=1)
            responses: list[Any] = pipeline.execute()
        # Each response is a list of (entry id, payload) pairs - at most one here.
        return {
            job_name: cls.deserialize(decode_dict(response[0][1], set()))
            for job_name, response in zip(job_names, responses)
            if response
        }
