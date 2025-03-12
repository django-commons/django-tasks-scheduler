from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

from scheduler import settings
from scheduler.helpers.queues import Queue, get_queue
from .broker_types import ConnectionType
from .redis_models import Callback

JOB_METHODS_LIST = list()


class job:
    queue_class = Queue

    def __init__(
            self,
            queue: Union["Queue", str, None] = None,
            connection: Optional[ConnectionType] = None,
            timeout: Optional[int] = settings.SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT,
            result_ttl: int = settings.SCHEDULER_CONFIG.DEFAULT_RESULT_TTL,
            ttl: Optional[int] = None,
            at_front: bool = False,
            meta: Optional[Dict[Any, Any]] = None,
            description: Optional[str] = None,
            retries_left: Optional[int] = None,
            retry_intervals: Union[int, List[int], None] = None,
            on_failure: Optional[Union[Callback, Callable[..., Any]]] = None,
            on_success: Optional[Union[Callback, Callable[..., Any]]] = None,
            on_stopped: Optional[Union[Callback, Callable[..., Any]]] = None,
    ):
        """A decorator that adds a ``delay`` method to the decorated function, which in turn creates a RQ job when
        called. Accepts a required ``queue`` argument that can be either a ``Queue`` instance or a string
        denoting the queue name.  For example::


        >>> @job(queue='default')
        >>> def simple_add(x, y):
        >>>    return x + y
        >>> ...
        >>> # Puts `simple_add` function into queue
        >>> simple_add.delay(1, 2)

        :param queue: The queue to use, can be the Queue class itself, or the queue name (str)
        :type queue: Union['Queue', str]
        :param connection: Broker Connection
        :param timeout: Job timeout
        :param result_ttl: Result time to live
        :param ttl: Time to live for job execution
        :param at_front: Whether to enqueue the job at front of the queue
        :param meta: Arbitraty metadata about the job
        :param description: Job description
        :param retries_left: Number of retries left
        :param retry_intervals: Retry intervals
        :param on_failure: Callable to run on failure
        :param on_success: Callable to run on success
        :param on_stopped: Callable to run when stopped
        """
        if queue is None:
            queue = "default"
        self.queue = get_queue(queue) if isinstance(queue, str) else queue
        self.connection = connection
        self.timeout = timeout
        self.result_ttl = result_ttl
        self.ttl = ttl
        self.meta = meta
        self.at_front = at_front
        self.description = description
        self.retries_left = retries_left
        self.retry_intervals = retry_intervals
        self.on_success = on_success
        self.on_failure = on_failure
        self.on_stopped = on_stopped

    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):
            if isinstance(self.queue, str):
                queue = Queue(name=self.queue, connection=self.connection)
            else:
                queue = self.queue

            job_id = kwargs.pop("job_id", None)
            at_front = kwargs.pop("at_front", False)

            if not at_front:
                at_front = self.at_front

            return queue.enqueue_call(
                f,
                args=args,
                kwargs=kwargs,
                timeout=self.timeout,
                result_ttl=self.result_ttl,
                ttl=self.ttl,
                name=job_id,
                at_front=at_front,
                meta=self.meta,
                description=self.description,
                retries_left=self.retries_left,
                retry_intervals=self.retry_intervals,
                on_failure=self.on_failure,
                on_success=self.on_success,
                on_stopped=self.on_stopped,
            )

        JOB_METHODS_LIST.append(f"{f.__module__}.{f.__name__}")
        f.delay = delay
        return f
