import base64
import dataclasses
import inspect
import numbers
import pickle
from datetime import datetime
from enum import Enum
from typing import ClassVar, Dict, Optional, List, Callable, Any, Union, Tuple

from scheduler.helpers import utils
from scheduler.helpers.callback import Callback
from scheduler.redis_models.base import HashModel, as_str
from scheduler.settings import SCHEDULER_CONFIG, logger
from scheduler.types import ConnectionType, Self, FunctionReferenceType
from .registry.base_registry import JobNamesRegistry
from ..helpers.utils import current_timestamp


class TimeoutFormatError(Exception):
    pass


class JobStatus(str, Enum):
    """The Status of Job within its lifecycle at any given time."""

    QUEUED = "queued"
    FINISHED = "finished"
    FAILED = "failed"
    STARTED = "started"
    SCHEDULED = "scheduled"
    STOPPED = "stopped"
    CANCELED = "canceled"


@dataclasses.dataclass(slots=True, kw_only=True)
class JobModel(HashModel):
    _list_key: ClassVar[str] = ":jobs:ALL:"
    _children_key_template: ClassVar[str] = ":{}:jobs:"
    _element_key_template: ClassVar[str] = ":jobs:{}"
    _non_serializable_fields = {"args", "kwargs"}

    args: List[Any]
    kwargs: Dict[str, str]

    queue_name: str
    description: str
    func_name: str

    timeout: int = SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT
    success_ttl: int = SCHEDULER_CONFIG.DEFAULT_SUCCESS_TTL
    job_info_ttl: int = SCHEDULER_CONFIG.DEFAULT_JOB_TTL
    status: JobStatus
    created_at: datetime
    meta: Dict[str, str]
    at_front: bool = False
    last_heartbeat: Optional[datetime] = None
    worker_name: Optional[str] = None
    started_at: Optional[datetime] = None
    enqueued_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    success_callback_name: Optional[str] = None
    success_callback_timeout: int = SCHEDULER_CONFIG.CALLBACK_TIMEOUT
    failure_callback_name: Optional[str] = None
    failure_callback_timeout: int = SCHEDULER_CONFIG.CALLBACK_TIMEOUT
    stopped_callback_name: Optional[str] = None
    stopped_callback_timeout: int = SCHEDULER_CONFIG.CALLBACK_TIMEOUT
    task_type: Optional[str] = None
    scheduled_task_id: Optional[int] = None

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):  # noqa
        return isinstance(other, self.__class__) and self.name == other.name

    def __str__(self):
        return f"{self.name}: {self.description}"

    def get_status(self, connection: ConnectionType) -> JobStatus:
        return self.get_field("status", connection=connection)

    @property
    def is_queued(self) -> bool:
        return self.status == JobStatus.QUEUED

    @property
    def is_canceled(self) -> bool:
        return self.status == JobStatus.CANCELED

    @property
    def is_failed(self) -> bool:
        return self.status == JobStatus.FAILED

    @property
    def func(self) -> Callable[[Any], Any]:
        return utils.callable_func(self.func_name)

    @property
    def is_scheduled_task(self) -> bool:
        return self.scheduled_task_id is not None

    def expire(self, ttl: int, connection: ConnectionType) -> None:
        """Expire the Job Model if ttl >= 0"""
        if ttl == 0:
            self.delete(connection=connection)
        elif ttl > 0:
            connection.expire(self._key, ttl)

    def persist(self, connection: ConnectionType) -> None:
        connection.persist(self._key)

    def prepare_for_execution(self, worker_name: str, registry: JobNamesRegistry, connection: ConnectionType) -> None:
        """Prepares the job for execution, setting the worker name,
        heartbeat information, status and other metadata before execution begins.
        :param worker_name: The name of the worker
        :param registry: The registry to add the job to
        :param current_pid: The current process id
        :param connection: The connection to the broker
        """
        self.worker_name = worker_name
        self.last_heartbeat = utils.utcnow()
        self.started_at = self.last_heartbeat
        self.status = JobStatus.STARTED
        registry.add(connection, self.name, self.last_heartbeat.timestamp())
        self.save(connection=connection)

    def after_execution(
        self,
        job_info_ttl: int,
        status: JobStatus,
        connection: ConnectionType,
        prev_registry: Optional[JobNamesRegistry] = None,
        new_registry: Optional[JobNamesRegistry] = None,
    ) -> None:
        """After the job is executed, update the status, heartbeat, and other metadata."""
        self.status = status
        self.ended_at = utils.utcnow()
        self.last_heartbeat = self.ended_at
        if prev_registry is not None:
            prev_registry.delete(connection, self.name)
        if new_registry is not None and job_info_ttl != 0:
            new_registry.add(connection, self.name, current_timestamp() + job_info_ttl)
        self.save(connection=connection)

    @property
    def failure_callback(self) -> Optional[Callback]:
        if self.failure_callback_name is None:
            return None
        logger.debug(f"Running failure callbacks for {self.name}")
        return Callback(self.failure_callback_name, self.failure_callback_timeout)

    @property
    def success_callback(self) -> Optional[Callable[..., Any]]:
        if self.success_callback_name is None:
            return None
        logger.debug(f"Running success callbacks for {self.name}")
        return Callback(self.success_callback_name, self.success_callback_timeout)

    @property
    def stopped_callback(self) -> Optional[Callable[..., Any]]:
        if self.stopped_callback_name is None:
            return None
        logger.debug(f"Running stopped callbacks for {self.name}")
        return Callback(self.stopped_callback_name, self.stopped_callback_timeout)

    def get_call_string(self):
        return _get_call_string(self.func_name, self.args, self.kwargs)

    def serialize(self, with_nones: bool = False) -> Dict[str, str]:
        """Serialize the job model to a dictionary."""
        res = super(JobModel, self).serialize(with_nones=with_nones)
        res["args"] = base64.encodebytes(pickle.dumps(self.args)).decode("utf-8")
        res["kwargs"] = base64.encodebytes(pickle.dumps(self.kwargs)).decode("utf-8")
        return res

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> Self:
        """Deserialize the job model from a dictionary."""
        res = super(JobModel, cls).deserialize(data)
        res.args = pickle.loads(base64.decodebytes(data.get("args").encode("utf-8")))
        res.kwargs = pickle.loads(base64.decodebytes(data.get("kwargs").encode("utf-8")))
        return res

    @classmethod
    def create(
        cls,
        connection: ConnectionType,
        func: FunctionReferenceType,
        queue_name: str,
        args: Union[List[Any], Optional[Tuple]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        result_ttl: Optional[int] = None,
        job_info_ttl: Optional[int] = None,
        status: Optional[JobStatus] = None,
        description: Optional[str] = None,
        timeout: Optional[int] = None,
        name: Optional[str] = None,
        task_type: Optional[str] = None,
        scheduled_task_id: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
        *,
        on_success: Optional[Callback] = None,
        on_failure: Optional[Callback] = None,
        on_stopped: Optional[Callback] = None,
        at_front: Optional[bool] = None,
    ) -> Self:
        """Creates a new job-model for the given function, arguments, and keyword arguments.
        :returns: A job-model instance.
        """
        args = args or []
        kwargs = kwargs or {}
        timeout = _parse_timeout(timeout) or SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT
        if timeout == 0:
            raise ValueError("0 timeout is not allowed. Use -1 for infinite timeout")
        job_info_ttl = _parse_timeout(job_info_ttl if job_info_ttl is not None else SCHEDULER_CONFIG.DEFAULT_JOB_TTL)
        result_ttl = _parse_timeout(result_ttl)
        if not isinstance(args, (tuple, list)):
            raise TypeError(f"{args!r} is not a valid args list")
        if not isinstance(kwargs, dict):
            raise TypeError(f"{kwargs!r} is not a valid kwargs dict")
        if on_success and not isinstance(on_success, Callback):
            raise ValueError("on_success must be a Callback object")
        if on_failure and not isinstance(on_failure, Callback):
            raise ValueError("on_failure must be a Callback object")
        if on_stopped and not isinstance(on_stopped, Callback):
            raise ValueError("on_stopped must be a Callback object")
        if name is not None and JobModel.exists(name, connection=connection):
            raise ValueError(f"Job with name {name} already exists")
        if name is None:
            date_str = utils.utcnow().strftime("%Y%m%d%H%M%S%f")
            name = f"{queue_name}:{scheduled_task_id or ''}:{date_str}"

        if inspect.ismethod(func):
            _func_name = func.__name__

        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            _func_name = f"{func.__module__}.{func.__qualname__}"
        elif isinstance(func, str):
            _func_name = as_str(func)
        elif not inspect.isclass(func) and hasattr(func, "__call__"):  # a callable class instance
            _func_name = "__call__"
        else:
            raise TypeError(f"Expected a callable or a string, but got: {func}")
        description = description or _get_call_string(func, args or [], kwargs or {}, max_length=75)
        job_info_ttl = job_info_ttl if job_info_ttl is not None else SCHEDULER_CONFIG.DEFAULT_JOB_TTL
        model = JobModel(
            created_at=utils.utcnow(),
            name=name,
            queue_name=queue_name,
            description=description,
            func_name=_func_name,
            args=args or [],
            kwargs=kwargs or {},
            at_front=at_front,
            task_type=task_type,
            scheduled_task_id=scheduled_task_id,
            success_callback_name=on_success.name if on_success else None,
            success_callback_timeout=on_success.timeout if on_success else None,
            failure_callback_name=on_failure.name if on_failure else None,
            failure_callback_timeout=on_failure.timeout if on_failure else None,
            stopped_callback_name=on_stopped.name if on_stopped else None,
            stopped_callback_timeout=on_stopped.timeout if on_stopped else None,
            success_ttl=result_ttl,
            job_info_ttl=job_info_ttl,
            timeout=timeout,
            status=status,
            last_heartbeat=None,
            meta=meta or {},
            worker_name=None,
            enqueued_at=None,
            started_at=None,
            ended_at=None,
        )
        model.save(connection=connection)
        return model


def _get_call_string(
    func_name: Optional[str], args: Any, kwargs: Dict[Any, Any], max_length: Optional[int] = None
) -> Optional[str]:
    """
    Returns a string representation of the call, formatted as a regular
    Python function invocation statement. If max_length is not None, truncate
    arguments with representation longer than max_length.

    :param func_name: The function name
    :param args: The function arguments
    :param kwargs: The function kwargs
    :param max_length: The max length of the return string
    :return: A string representation of the function call
    """
    if func_name is None:
        return None

    arg_list = [as_str(_truncate_long_string(repr(arg), max_length)) for arg in args]

    list_kwargs = [f"{k}={as_str(_truncate_long_string(repr(v), max_length))}" for k, v in kwargs.items()]
    arg_list += sorted(list_kwargs)
    args = ", ".join(arg_list)

    return f"{func_name}({args})"


def _truncate_long_string(data: str, max_length: Optional[int] = None) -> str:
    """Truncate arguments with representation longer than max_length"""
    if max_length is None:
        return data
    return (data[:max_length] + "...") if len(data) > max_length else data


def _parse_timeout(timeout: Union[int, float, str]) -> int:
    """Transfer all kinds of timeout format to an integer representing seconds"""
    if not isinstance(timeout, numbers.Integral) and timeout is not None:
        try:
            timeout = int(timeout)
        except ValueError:
            digit, unit = timeout[:-1], (timeout[-1:]).lower()
            unit_second = {"d": 86400, "h": 3600, "m": 60, "s": 1}
            try:
                timeout = int(digit) * unit_second[unit]
            except (ValueError, KeyError):
                raise TimeoutFormatError(
                    "Timeout must be an integer or a string representing an integer, or "
                    'a string with format: digits + unit, unit can be "d", "h", "m", "s", '
                    'such as "1h", "23m".'
                )

    return timeout
