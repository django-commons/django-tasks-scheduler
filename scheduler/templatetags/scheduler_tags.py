from typing import Dict, Optional

from django import template
from django.utils.safestring import mark_safe

from scheduler.helpers.queues import Queue
from scheduler.models import Task, get_scheduled_task
from scheduler.models.task import run_task
from scheduler.redis_models import Result, JobModel
from scheduler.views.helpers import get_queue

register = template.Library()


@register.filter
def show_func_name(job: JobModel) -> str:
    try:
        res = job.func_name
        if job.func == run_task:
            task = get_scheduled_task(*job.args)
            res = task.function_string()
        return mark_safe(res)
    except Exception as e:
        return repr(e)


@register.filter
def get_item(dictionary: Dict, key):
    return dictionary.get(key)


@register.filter
def scheduled_task(job: JobModel) -> Task:
    django_scheduled_task = get_scheduled_task(*job.args)
    return django_scheduled_task.get_absolute_url()


@register.filter
def job_result(job: JobModel) -> Optional[str]:
    queue = get_queue(job.queue_name)
    result = Result.fetch_latest(queue.connection, job.name)
    return result.type.name.capitalize() if result is not None else None


@register.filter
def job_scheduled_task(job: JobModel) -> Optional[str]:
    task = Task.objects.filter(id=job.scheduled_task_id).first()
    return task.name if task is not None else None


@register.filter
def job_status(job: JobModel):
    result = job.status
    return result.capitalize()


@register.filter
def job_runtime(job: JobModel):
    ended_at = job.ended_at
    if ended_at:
        runtime = job.ended_at - job.started_at
        return f"{int(runtime.microseconds / 1000)}ms"
    elif job.started_at:
        return "Still running"
    else:
        return "-"


@register.filter
def job_scheduled_time(job: JobModel, queue: Queue):
    return queue.scheduled_job_registry.get_scheduled_time(job.name)
