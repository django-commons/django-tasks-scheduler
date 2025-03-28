"""Helper functions for the scheduler that require django loaded"""

import os
from typing import Any, Optional

import croniter
from django.apps import apps
from django.utils import timezone

from scheduler.broker_types import TASK_TYPES
from scheduler.helpers.queues import get_queues
from scheduler.models.task import Task, TaskType
from scheduler.redis_models import WorkerModel
from scheduler.settings import SCHEDULER_CONFIG, Broker, logger
from scheduler.worker.worker import Worker


def get_next_cron_time(cron_string: Optional[str]) -> Optional[timezone.datetime]:
    """Calculate the next scheduled time by creating a crontab object with a cron string"""
    if cron_string is None:
        return None
    now = timezone.now()
    itr = croniter.croniter(cron_string, now)
    next_itr = itr.get_next(timezone.datetime)
    return next_itr


def get_scheduled_task(task_type_str: str, task_id: int) -> Task:
    # Try with new model names
    model = apps.get_model(app_label="scheduler", model_name="Task")
    if task_type_str in TASK_TYPES:
        try:
            task_type = TaskType(task_type_str)
            task = model.objects.filter(task_type=task_type, id=task_id).first()
            if task is None:
                raise ValueError(f"Job {task_type}:{task_id} does not exit")
            return task
        except ValueError:
            raise ValueError(f"Invalid task type {task_type_str}")
    raise ValueError(f"Job Model {task_type_str} does not exist, choices are {TASK_TYPES}")


def run_task(task_model: str, task_id: int) -> Any:
    """Run a scheduled job"""
    if isinstance(task_id, str):
        task_id = int(task_id)
    scheduled_task = get_scheduled_task(task_model, task_id)
    logger.debug(f"Running task {str(scheduled_task)}")
    args = scheduled_task.parse_args()
    kwargs = scheduled_task.parse_kwargs()
    res = scheduled_task.callable_func()(*args, **kwargs)
    return res


def _calc_worker_name(existing_worker_names) -> str:
    hostname = os.uname()[1]
    c = 1
    worker_name = f"{hostname}-worker.{c}"
    while worker_name in existing_worker_names:
        c += 1
        worker_name = f"{hostname}-worker.{c}"
    return worker_name


def create_worker(*queue_names: str, **kwargs) -> Worker:
    """Returns a Django worker for all queues or specified ones."""
    queues = get_queues(*queue_names)
    existing_worker_names = WorkerModel.all_names(connection=queues[0].connection)
    kwargs.setdefault("fork_job_execution", SCHEDULER_CONFIG.BROKER != Broker.FAKEREDIS)
    if kwargs.get("name", None) is None:
        kwargs["name"] = _calc_worker_name(existing_worker_names)
    if kwargs["name"] in existing_worker_names:
        raise ValueError(f"Worker {kwargs['name']} already exists")
    kwargs["name"] = kwargs["name"].replace("/", ".")
    kwargs.setdefault("with_scheduler", False)
    worker = Worker(queues, connection=queues[0].connection, **kwargs)
    return worker
