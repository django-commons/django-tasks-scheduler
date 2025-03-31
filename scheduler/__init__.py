import importlib.metadata

__version__ = importlib.metadata.version("django-tasks-scheduler")

__all__ = [
    "QueueConfiguration",
    "SchedulerConfiguration",
    "job",
]

from scheduler.settings_types import QueueConfiguration, SchedulerConfiguration
from scheduler.decorators import job
