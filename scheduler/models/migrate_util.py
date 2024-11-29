from datetime import datetime
from typing import Dict, Any, Optional

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.utils import timezone

from scheduler.models.old_scheduled_task import BaseTask
from scheduler.models.task import Task, TaskArg, TaskKwarg
from scheduler.settings import logger
from scheduler.tools import TaskType


def job_model_str(model_str: str) -> str:
    if model_str.find("Job") == len(model_str) - 3:
        return model_str[:-3] + "Task"
    return model_str


def get_task_type(model_str: str) -> TaskType:
    model_str = job_model_str(model_str)
    if TaskType(model_str):
        return TaskType(model_str)
    if model_str == "CronTask":
        return TaskType.CRON
    elif model_str == "RepeatableTask":
        return TaskType.REPEATABLE
    elif model_str in {"ScheduledTask", "OnceTask"}:
        return TaskType.ONCE
    raise ValueError(f"Invalid model {model_str}")


def create_task_from_dict(task_dict: Dict[str, Any], recreate: bool) -> Optional[Task]:
    if "new_task_id" in task_dict:
        existing_task = Task.objects.filter(id=task_dict["new_task_id"]).first()
    else:
        existing_task = Task.objects.filter(name=task_dict["name"]).first()
    task_type = get_task_type(task_dict["model"])
    if existing_task:
        if recreate:
            logger.info(f'Found existing job "{existing_task}, removing it to be reinserted"')
            existing_task.delete()
        else:
            logger.info(f'Found existing job "{existing_task}", skipping')
            return None
    kwargs = dict(task_dict)
    kwargs["task_type"] = task_type
    del kwargs["model"]
    del kwargs["callable_args"]
    del kwargs["callable_kwargs"]
    del kwargs["new_task_id"]
    if kwargs.get("scheduled_time", None):
        target = datetime.fromisoformat(kwargs["scheduled_time"])
        if not settings.USE_TZ and not timezone.is_naive(target):
            target = timezone.make_naive(target)
        kwargs["scheduled_time"] = target
    model_fields = filter(lambda field: hasattr(field, 'attname'), Task._meta.get_fields())
    model_fields = set(map(lambda field: field.attname, model_fields))
    keys_to_ignore = list(filter(lambda _k: _k not in model_fields, kwargs.keys()))
    for k in keys_to_ignore:
        del kwargs[k]
    task = Task.objects.create(**kwargs)
    logger.info(f"Created task {task}")
    content_type = ContentType.objects.get_for_model(task)

    for arg in task_dict["callable_args"]:
        TaskArg.objects.create(
            content_type=content_type,
            object_id=task.id,
            **arg,
        )
    for arg in task_dict["callable_kwargs"]:
        TaskKwarg.objects.create(
            content_type=content_type,
            object_id=task.id,
            **arg,
        )
    return task


def migrate(old: BaseTask) -> Optional[Task]:
    old_task_dict = old.to_dict()
    new_task = create_task_from_dict(old_task_dict, old_task_dict.get("new_task_id") is not None)
    old.new_task_id = new_task.id
    old.enabled = False
    old.save()
    return new_task
