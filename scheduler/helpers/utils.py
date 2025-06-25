import datetime
import importlib
import time
from typing import Callable, Any


def current_timestamp() -> int:
    """Returns current UTC timestamp in secs"""
    return int(time.time())


def utcnow() -> datetime.datetime:
    """Return now in UTC"""
    return datetime.datetime.now(datetime.timezone.utc)


def callable_func(callable_str: str) -> Callable[[Any], Any]:
    path = callable_str.split(".")
    module = importlib.import_module(".".join(path[:-1]))
    func: Callable[[Any], Any] = getattr(module, path[-1])
    if not callable(func):
        raise TypeError(f"'{callable_str}' is not callable")
    return func
