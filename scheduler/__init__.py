import importlib.metadata

__version__ = importlib.metadata.version('django-tasks-scheduler')

from .decorators import job  # noqa: F401
