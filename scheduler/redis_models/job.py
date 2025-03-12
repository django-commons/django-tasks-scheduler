import dataclasses
import inspect
import numbers
from datetime import datetime
from enum import Enum
from typing import ClassVar, Dict, Optional, List, Callable, Any, Union, Tuple, Self, Iterable
from uuid import uuid4

from scheduler.helpers import utils
from scheduler.broker_types import ConnectionType, FunctionReferenceType
from scheduler.redis_models import Callback
from scheduler.redis_models.base import HashModel, as_str
from scheduler.redis_models.registry.base_registry import JobNamesRegistry
from scheduler.settings import SCHEDULER_CONFIG


class TimeoutFormatError(Exception):
    pass


def get_call_string(
        func_name: Optional[str], args: Any, kwargs: Dict[Any, Any], max_length: Optional[int] = None
) -> Optional[str]:
    """
    Returns a string representation of the call, formatted as a regular
    Python function invocation statement. If max_length is not None, truncate
    arguments with representation longer than max_length.

    Args:
        func_name (str): The funtion name
        args (Any): The function arguments
        kwargs (Dict[Any, Any]): The function kwargs
        max_length (int, optional): The max length. Defaults to None.

    Returns:
        str: A String representation of the function call.
    """
    if func_name is None:
        return None

    arg_list = [as_str(_truncate_long_string(repr(arg), max_length)) for arg in args]

    list_kwargs = ["{0}={1}".format(k, as_str(_truncate_long_string(repr(v), max_length))) for k, v in kwargs.items()]
    arg_list += sorted(list_kwargs)
    args = ", ".join(arg_list)

    return "{0}({1})".format(func_name, args)


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
    _list_key: ClassVar[str] = ":jobs:"
    _children_key_template: ClassVar[str] = ":{}:jobs:"
    _element_key_template: ClassVar[str] = ":jobs:{}"

    queue_name: str
    description: str
    func_name: str

    args: List[Any]
    kwargs: Dict[str, str]
    timeout: int = SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT
    result_ttl: int = SCHEDULER_CONFIG.DEFAULT_RESULT_TTL
    ttl: int = SCHEDULER_CONFIG.DEFAULT_WORKER_TTL
    status: JobStatus
    created_at: datetime
    meta: Dict[str, str]
    at_front: bool = False
    retries_left: Optional[int] = None
    retry_intervals: Optional[List[int]] = None
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

    def serialize(self) -> Dict[str, str]:
        res = super(JobModel, self).serialize()
        return res

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):  # noqa
        return isinstance(other, self.__class__) and self.name == other.name

    def __str__(self):
        return f"{self.name}: {self.description}"

    def get_status(self, connection: ConnectionType) -> JobStatus:
        broker_value = self.get_field("status", connection=connection)
        return JobStatus(broker_value)

    def set_status(self, status: JobStatus, connection: ConnectionType) -> None:
        """Set's the Job Status"""
        self.set_field("status", status, connection=connection)

    def is_execution_of(self, task: "Task") -> bool:
        return self.scheduled_task_id == task.id and self.task_type == task.task_type

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
        :param connection: The connection to the broker
        """
        self.worker_name = worker_name
        self.last_heartbeat = utils.utcnow()
        self.started_at = self.last_heartbeat
        self.status = JobStatus.STARTED
        registry.add(connection, self.name, self.last_heartbeat.timestamp())
        self.save(connection=connection)

    @property
    def failure_callback(self) -> Optional[Callback]:
        if self.failure_callback_name is None:
            return None
        return Callback(self.failure_callback_name, self.failure_callback_timeout)

    @property
    def success_callback(self) -> Optional[Callable[..., Any]]:
        if self.success_callback_name is None:
            return None
        return Callback(self.success_callback_name, self.success_callback_timeout)

    @property
    def stopped_callback(self) -> Optional[Callable[..., Any]]:
        if self.stopped_callback_name is None:
            return None
        return Callback(self.stopped_callback_name, self.stopped_callback_timeout)

    def get_call_string(self):
        return get_call_string(self.func_name, self.args, self.kwargs)


    @classmethod
    def create(
            cls,
            connection: ConnectionType,
            func: FunctionReferenceType,
            queue_name: str,
            args: Union[List[Any], Optional[Tuple]] = None,
            kwargs: Optional[Dict[str, Any]] = None,
            result_ttl: Optional[int] = None,
            ttl: Optional[int] = None,
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
            retries_left: Optional[int] = None,
            retry_intervals: Union[int, List[int], None] = None,
    ) -> Self:
        """Creates a new job-model for the given function, arguments, and keyword arguments.
        :returns: A job-model instance.
        """
        args = args or []
        kwargs = kwargs or {}
        timeout = _parse_timeout(timeout) or SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT
        if timeout == 0:
            raise ValueError("0 timeout is not allowed. Use -1 for infinite timeout")
        ttl = _parse_timeout(ttl or SCHEDULER_CONFIG.DEFAULT_RESULT_TTL)
        if ttl is not None and ttl <= 0:
            raise ValueError("Job ttl must be greater than 0")
        result_ttl = _parse_timeout(result_ttl)
        if not isinstance(args, (tuple, list)):
            raise TypeError("{0!r} is not a valid args list".format(args))
        if not isinstance(kwargs, dict):
            raise TypeError("{0!r} is not a valid kwargs dict".format(kwargs))
        if on_success and not isinstance(on_success, Callback):
            raise ValueError("on_success must be a Callback object")
        if on_failure and not isinstance(on_failure, Callback):
            raise ValueError("on_failure must be a Callback object")
        if on_stopped and not isinstance(on_stopped, Callback):
            raise ValueError("on_stopped must be a Callback object")
        name = name or str(uuid4())

        if inspect.ismethod(func):
            _func_name = func.__name__

        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            _func_name = "{0}.{1}".format(func.__module__, func.__qualname__)
        elif isinstance(func, str):
            _func_name = as_str(func)
        elif not inspect.isclass(func) and hasattr(func, "__call__"):  # a callable class instance
            _func_name = "__call__"
        else:
            raise TypeError("Expected a callable or a string, but got: {0}".format(func))
        description = description or get_call_string(func, args or [], kwargs or {}, max_length=75)

        if retries_left is not None and retries_left < 1:
            raise ValueError("max: please enter a value greater than 0")
        if retry_intervals is None:
            pass
        elif isinstance(retry_intervals, int):
            if retry_intervals < 0:
                raise ValueError("interval: negative numbers are not allowed")
            retry_intervals = [retry_intervals]
        elif isinstance(retry_intervals, Iterable):
            for i in retry_intervals:
                if i < 0:
                    raise ValueError("interval: negative numbers are not allowed")
            retry_intervals = retry_intervals

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
            result_ttl=result_ttl,
            ttl=ttl or SCHEDULER_CONFIG.DEFAULT_RESULT_TTL,
            timeout=timeout,
            status=status,
            last_heartbeat=None,
            meta=meta or {},
            retry_intervals=retry_intervals,
            retries_left=retries_left,
            worker_name=None,
            enqueued_at=None,
            started_at=None,
            ended_at=None,
        )
        model.save(connection=connection)
        return model


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
