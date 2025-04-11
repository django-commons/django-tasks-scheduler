import datetime
import importlib
import time
from typing import Callable


def current_timestamp() -> int:
    """Returns current UTC timestamp in secs"""
    return int(time.time())


def utcnow() -> datetime.datetime:
    """Return now in UTC"""
    return datetime.datetime.now(datetime.timezone.utc)


def callable_func(callable_str: str) -> Callable:
    path = callable_str.split(".")
    module = importlib.import_module(".".join(path[:-1]))
    func = getattr(module, path[-1])
    if callable(func) is False:
        raise TypeError(f"'{callable_str}' is not callable")
    return func
