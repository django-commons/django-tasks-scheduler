from typing import Dict, Optional

from django import template
from django.utils.safestring import mark_safe

from scheduler.rq_classes import JobExecution, DjangoQueue, DjangoWorker
from scheduler.tools import get_scheduled_task

register = template.Library()


@register.filter
def show_func_name(rq_job: JobExecution) -> str:
    try:
        res = rq_job.func_name
        if res == "scheduler.tools.run_task":
            task = get_scheduled_task(*rq_job.args)
            res = task.function_string()
        return mark_safe(res)
    except Exception as e:
        return repr(e)


@register.filter
def get_item(dictionary: Dict, key):
    return dictionary.get(key)


@register.filter
def scheduled_job(job: JobExecution):
    django_scheduled_job = get_scheduled_task(*job.args)
    return django_scheduled_job.get_absolute_url()


@register.filter
def worker_scheduler_pid(worker: Optional[DjangoWorker]) -> str:
    scheduler_pid = worker.scheduler_pid() if worker is not None else None
    return str(scheduler_pid) if scheduler_pid is not None else "-"


@register.filter
def job_result(job: JobExecution):
    result = job.latest_result()
    return result.type.name.capitalize() if result else None


@register.filter
def job_status(job: JobExecution):
    result = job.get_status()
    return result.capitalize()


@register.filter
def job_runtime(job: JobExecution):
    ended_at = job.ended_at
    if ended_at:
        runtime = job.ended_at - job.started_at
        return f"{int(runtime.microseconds / 1000)}ms"
    elif job.started_at:
        return "Still running"
    else:
        return "-"


@register.filter
def job_scheduled_time(job: JobExecution, queue: DjangoQueue):
    try:
        return queue.scheduled_job_registry.get_scheduled_time(job.id)
    except Exception:
        return None
