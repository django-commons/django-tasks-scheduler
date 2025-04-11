import inspect
from typing import Union, Callable, Any, Optional

from scheduler.helpers.utils import callable_func
from scheduler.timeouts import JobTimeoutException


class CallbackSetupError(Exception):
    pass


class Callback:
    def __init__(self, func: Union[str, Callable[..., Any]], timeout: Optional[int] = None):
        from scheduler.settings import SCHEDULER_CONFIG

        self.timeout = timeout or SCHEDULER_CONFIG.CALLBACK_TIMEOUT
        if not isinstance(self.timeout, int) or self.timeout < 0:
            raise CallbackSetupError(f"Callback `timeout` must be a positive int, but received {self.timeout}")
        if not isinstance(func, str) and not inspect.isfunction(func) and not inspect.isbuiltin(func):
            raise CallbackSetupError(f"Callback `func` must be a string or function, received {func}")
        if isinstance(func, str):
            try:
                func_str = func
                func = callable_func(func)
            except (TypeError, AttributeError, ModuleNotFoundError, ValueError):
                raise CallbackSetupError(f"Callback `func` is not callable: {func_str}")
        self.func: Callable[..., Any] = func

    @property
    def name(self) -> str:
        return f"{self.func.__module__}.{self.func.__qualname__}"

    def __call__(self, *args, **kwargs):
        from scheduler.settings import SCHEDULER_CONFIG

        with SCHEDULER_CONFIG.DEATH_PENALTY_CLASS(self.timeout, JobTimeoutException):
            return self.func(*args, **kwargs)
