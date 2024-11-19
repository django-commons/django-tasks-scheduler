import importlib
import os
from typing import List, Any, Callable, Optional

import croniter
from django.apps import apps
from django.db import models
from django.utils import timezone
from django.utils.module_loading import import_string
from django.utils.translation import gettext_lazy as _

from scheduler.queues import get_queues, logger, get_queue
from scheduler.rq_classes import DjangoWorker, JobExecution, TASK_TYPES, MODEL_NAMES
from scheduler.settings import SCHEDULER_CONFIG, Broker


class TaskType(models.TextChoices):
    CRON = "CronTaskType", _("Cron Task")
    REPEATABLE = "RepeatableTaskType", _("Repeatable Task")
    ONCE = "OnceTaskType", _("Run once")


def callable_func(callable_str: str) -> Callable:
    path = callable_str.split(".")
    module = importlib.import_module(".".join(path[:-1]))
    func = getattr(module, path[-1])
    if callable(func) is False:
        raise TypeError("'{}' is not callable".format(callable_str))
    return func


def get_next_cron_time(cron_string: Optional[str]) -> Optional[timezone.datetime]:
    """Calculate the next scheduled time by creating a crontab object with a cron string"""
    if cron_string is None:
        return None
    now = timezone.now()
    itr = croniter.croniter(cron_string, now)
    next_itr = itr.get_next(timezone.datetime)
    return next_itr


def get_scheduled_task(task_type_str: str, task_id: int) -> "BaseTask":  # noqa: F821
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
    elif task_type_str in MODEL_NAMES:
        model = apps.get_model(app_label="scheduler", model_name=task_type_str)
        task = model.objects.filter(id=task_id).first()
        if task is None:
            raise ValueError(f"Job {task_type_str}:{task_id} does not exit")
        return task
    raise ValueError(f"Job Model {task_type_str} does not exist, choices are {TASK_TYPES}")


def run_task(task_model: str, task_id: int) -> Any:
    """Run a scheduled job"""
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


def create_worker(*queue_names, **kwargs) -> DjangoWorker:
    """Returns a Django worker for all queues or specified ones."""

    queues = get_queues(*queue_names)
    existing_workers = DjangoWorker.all(connection=queues[0].connection)
    existing_worker_names = set(map(lambda w: w.name, existing_workers))
    kwargs.setdefault("fork_job_execution", SCHEDULER_CONFIG.BROKER != Broker.FAKEREDIS)
    if kwargs.get("name", None) is None:
        kwargs["name"] = _calc_worker_name(existing_worker_names)

    kwargs["name"] = kwargs["name"].replace("/", ".")

    # Handle job_class if provided
    if "job_class" not in kwargs or kwargs["job_class"] is None:
        kwargs["job_class"] = "scheduler.rq_classes.JobExecution"
    try:
        kwargs["job_class"] = import_string(kwargs["job_class"])
    except ImportError:
        raise ImportError(f"Could not import job class {kwargs['job_class']}")

    worker = DjangoWorker(queues, connection=queues[0].connection, **kwargs)
    return worker


def get_job_executions_for_task(queue_name, scheduled_task) -> List[JobExecution]:
    queue = get_queue(queue_name)
    job_list = queue.get_all_jobs()
    res = list(filter(lambda j: j.is_execution_of(scheduled_task), job_list))
    return res
