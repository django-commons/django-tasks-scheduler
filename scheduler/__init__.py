import importlib.metadata

__version__ = importlib.metadata.version("django-tasks-scheduler")

__all__ = [
    "QueueConfiguration",
    "SchedulerConfig",
    "job",
]

from scheduler.settings_types import QueueConfiguration, SchedulerConfig
from scheduler.decorators import job
