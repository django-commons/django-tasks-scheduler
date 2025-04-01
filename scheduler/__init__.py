import importlib.metadata

__version__ = importlib.metadata.version("django-tasks-scheduler")

__all__ = [
    "job",
]

from scheduler.decorators import job
