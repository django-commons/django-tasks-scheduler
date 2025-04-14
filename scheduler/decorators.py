from functools import wraps
from typing import Any, Callable, Dict, Optional, Union, List

from scheduler.helpers.callback import Callback
from scheduler.types import ConnectionType

JOB_METHODS_LIST: List[str] = list()


class job:
    def __init__(
        self,
        queue: Union["Queue", str, None] = None,  # noqa: F821
        connection: Optional[ConnectionType] = None,
        timeout: Optional[int] = None,
        result_ttl: Optional[int] = None,
        job_info_ttl: Optional[int] = None,
        at_front: bool = False,
        meta: Optional[Dict[Any, Any]] = None,
        description: Optional[str] = None,
        on_failure: Optional[Union["Callback", Callable[..., Any]]] = None,
        on_success: Optional[Union["Callback", Callable[..., Any]]] = None,
        on_stopped: Optional[Union["Callback", Callable[..., Any]]] = None,
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
        :param job_info_ttl: Time to live for job info
        :param at_front: Whether to enqueue the job at front of the queue
        :param meta: Arbitraty metadata about the job
        :param description: Job description
        :param on_failure: Callable to run on failure
        :param on_success: Callable to run on success
        :param on_stopped: Callable to run when stopped
        """
        from scheduler.helpers.queues import get_queue

        if queue is None:
            queue = "default"
        self.queue = get_queue(queue) if isinstance(queue, str) else queue
        self.connection = connection
        self.timeout = timeout
        self.result_ttl = result_ttl
        self.job_info_ttl = job_info_ttl
        self.meta = meta
        self.at_front = at_front
        self.description = description
        self.on_success = on_success
        self.on_failure = on_failure
        self.on_stopped = on_stopped

    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):
            from scheduler.helpers.queues import get_queue

            queue = get_queue(self.queue) if isinstance(self.queue, str) else self.queue

            job_name = kwargs.pop("job_name", None)
            at_front = kwargs.pop("at_front", False)

            if not at_front:
                at_front = self.at_front

            return queue.create_and_enqueue_job(
                f,
                args=args,
                kwargs=kwargs,
                timeout=self.timeout,
                result_ttl=self.result_ttl,
                job_info_ttl=self.job_info_ttl,
                name=job_name,
                at_front=at_front,
                meta=self.meta,
                description=self.description,
                on_failure=self.on_failure,
                on_success=self.on_success,
                on_stopped=self.on_stopped,
                when=None,
            )

        JOB_METHODS_LIST.append(f"{f.__module__}.{f.__name__}")
        f.delay = delay
        return f
