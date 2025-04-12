import dataclasses
from datetime import datetime
from enum import Enum
from typing import Optional, Any, ClassVar, List

from scheduler.helpers.utils import utcnow
from scheduler.redis_models.base import StreamModel, decode_dict
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
    ttl: Optional[int] = 0
    name: Optional[str] = None
    created_at: datetime = dataclasses.field(default_factory=utcnow)
    return_value: Optional[Any] = None
    exc_string: Optional[str] = None

    _list_key: ClassVar[str] = ":job-results:"
    _children_key_template: ClassVar[str] = ":job-results:{}:"
    _element_key_template: ClassVar[str] = ":job-results:{}"

    @classmethod
    def create(
        cls,
        connection: ConnectionType,
        job_name: str,
        worker_name: str,
        _type: ResultType,
        ttl: int,
        return_value: Any = None,
        exc_string: Optional[str] = None,
    ) -> Self:
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

    @classmethod
    def fetch_latest(cls, connection: ConnectionType, job_name: str) -> Optional["Result"]:
        """Returns the latest result for given job_name.

        :param connection: Broker connection.
        :param job_name: Job name.
        :return: Result instance or None if no result is available.
        """
        response: List[Any] = connection.xrevrange(cls._children_key_template.format(job_name), "+", "-", count=1)
        if not response:
            return None
        result_id, payload = response[0]
        res = cls.deserialize(decode_dict(payload, set()))
        return res
