import importlib
import inspect
from typing import Union, Callable, Any, Optional

from scheduler.settings import SCHEDULER_CONFIG
from scheduler.helpers.timeouts import JobTimeoutException


class Callback:
    def __init__(self, func: Union[str, Callable[..., Any]], timeout: Optional[int] = None):
        self.timeout = timeout or SCHEDULER_CONFIG.CALLBACK_TIMEOUT
        if not isinstance(self.timeout, int):
            raise ValueError("Callback `timeout` must be an int")
        if not isinstance(func, str) and not inspect.isfunction(func) and not inspect.isbuiltin(func):
            raise ValueError("Callback `func` must be a string or function")
        if isinstance(func, str):
            func = _import_attribute(func)
        self.func: Callable[..., Any] = func

    @property
    def name(self) -> str:
        return "{0}.{1}".format(self.func.__module__, self.func.__qualname__)

    def __call__(self, *args, **kwargs):
        with SCHEDULER_CONFIG.DEATH_PENALTY_CLASS(self.timeout, JobTimeoutException):
            return self.func(*args, **kwargs)


def _import_attribute(name: str) -> Callable[..., Any]:
    """Returns an attribute from a dotted path name. Example: `path.to.func`.

    When the attribute we look for is a staticmethod, module name in its dotted path is not the last-before-end word
    E.g.: package_a.package_b.module_a.ClassA.my_static_method
    Thus we remove the bits from the end of the name until we can import it

    :param name: The name (reference) to the path.
    :raises ValueError: If no module is found or invalid attribute name.
    :returns: An attribute (normally a Callable)
    """
    name_bits = name.split(".")
    module_name_bits, attribute_bits = name_bits[:-1], [name_bits[-1]]
    module = None
    while len(module_name_bits):
        try:
            module_name = ".".join(module_name_bits)
            module = importlib.import_module(module_name)
            break
        except ImportError:
            attribute_bits.insert(0, module_name_bits.pop())

    if module is None:
        # maybe it's a builtin
        try:
            return __builtins__[name]
        except KeyError:
            raise ValueError("Invalid attribute name: %s" % name)

    attribute_name = ".".join(attribute_bits)
    if hasattr(module, attribute_name):
        return getattr(module, attribute_name)
    # staticmethods
    attribute_name = attribute_bits.pop()
    attribute_owner_name = ".".join(attribute_bits)
    try:
        attribute_owner = getattr(module, attribute_owner_name)
    except:  # noqa
        raise ValueError("Invalid attribute name: %s" % attribute_name)

    if not hasattr(attribute_owner, attribute_name):
        raise ValueError("Invalid attribute name: %s" % name)
    return getattr(attribute_owner, attribute_name)
