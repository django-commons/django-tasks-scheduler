from scheduler import settings
from .queues import get_queue, QueueNotFoundError
from .rq_classes import rq_job_decorator

JOB_METHODS_LIST = list()


def job(*args, **kwargs):
    """
    The same as rq package's job decorator, but it automatically works out
    the ``connection`` argument from SCHEDULER_QUEUES.

    And also, it allows simplified ``@job`` syntax to put a job into the default queue.

    """
    if len(args) == 0:
        func = None
        queue = "default"
    else:
        if callable(args[0]):
            func = args[0]
            queue = "default"
        else:
            func = None
            queue = args[0]
        args = args[1:]

    if isinstance(queue, str):
        try:
            queue = get_queue(queue)
            if "connection" not in kwargs:
                kwargs["connection"] = queue.connection
        except KeyError:
            raise QueueNotFoundError(f"Queue {queue} does not exist")

    kwargs.setdefault("result_ttl", settings.SCHEDULER_CONFIG.DEFAULT_RESULT_TTL)
    kwargs.setdefault("timeout", settings.SCHEDULER_CONFIG.DEFAULT_TIMEOUT)

    decorator = rq_job_decorator(queue, *args, **kwargs)
    if func:
        JOB_METHODS_LIST.append(f"{func.__module__}.{func.__name__}")
        return decorator(func)
    return decorator
