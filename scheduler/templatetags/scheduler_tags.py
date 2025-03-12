from typing import Dict, Optional

from django import template
from django.utils.safestring import mark_safe

from scheduler.helpers.queues import Queue
from scheduler.helpers.tools import get_scheduled_task
from scheduler.models.task import Task
from scheduler.redis_models import Result, ResultType, JobModel, WorkerModel
from scheduler.views import get_queue

register = template.Library()


@register.filter
def show_func_name(job: JobModel) -> str:
    try:
        res = job.func_name
        if res == "scheduler.tools.run_task":
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
def worker_scheduler_pid(worker: WorkerModel) -> str:
    return str(worker.scheduler_pid) if worker.scheduler_pid is not None else "No Scheduler"


@register.filter
def job_result(job: JobModel) -> Optional[str]:
    queue = get_queue(job.queue_name)
    result = Result.fetch_latest(queue.connection, job.name)
    return result.type.name.capitalize() if result else None


@register.filter
def job_exc_info(job: JobModel) -> Optional[str]:
    queue = get_queue(job.queue_name)
    result = Result.fetch_latest(queue.connection, job.name)
    if result and result.type == ResultType.FAILED and result.exc_string:
        return mark_safe(result.exc_string)
    return None


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
    try:
        return queue.scheduled_job_registry.get_scheduled_time(job.name)
    except Exception:
        return None
