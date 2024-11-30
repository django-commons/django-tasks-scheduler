import importlib.metadata

__version__ = importlib.metadata.version("django-tasks-scheduler")

from .decorators import job

__all__ = [
    "job",
]
