from collections.abc import Sequence
from urllib.parse import urlparse

from django.contrib import messages
from django.http import Http404, HttpRequest
from django.urls import resolve
from django.utils.http import url_has_allowed_host_and_scheme

from scheduler.helpers.queues import Queue
from scheduler.helpers.queues import get_queue as get_queue_base
from scheduler.models import Task
from scheduler.models.task import run_task
from scheduler.redis_models import JobModel
from scheduler.settings import QueueNotFoundError, get_queue_names, logger


def get_queue(queue_name: str, fail_fast: bool = False) -> Queue:
    try:
        return get_queue_base(queue_name, fail_fast=fail_fast)
    except QueueNotFoundError as e:
        logger.error(e)
        raise Http404(e)


def _find_job(job_name: str) -> tuple[Queue | None, JobModel | None]:
    queue_names = get_queue_names()
    for queue_name in queue_names:
        try:
            queue = get_queue(queue_name, fail_fast=True)
            job = JobModel.get(job_name, connection=queue.connection)
            if job is not None and job.queue_name == queue_name:
                return queue, job
        except Exception as e:
            logger.debug(f"Could not check queue {queue_name} for job {job_name} - Got exception: {e}")
    return None, None


def _check_next_url(request: HttpRequest, default_next_url: str) -> str:
    next_url: str = request.POST.get("next_url", default_next_url)
    next_url = next_url.replace("\\", "")
    if (
        not url_has_allowed_host_and_scheme(next_url, allowed_hosts=None)
        or urlparse(next_url).netloc
        or urlparse(next_url).scheme
    ):
        messages.warning(request, "Bad followup URL")
        next_url = default_next_url
    try:
        resolve(next_url)
    except Exception:
        messages.warning(request, "Bad followup URL")
        next_url = default_next_url
    return next_url


def _call_strings(jobs: Sequence[JobModel]) -> dict[str, str]:
    """Maps each job's name to the call it makes: its scheduled task's function string, or else its function name.

    Reads all the jobs' tasks, with their arguments, in one go rather than a few queries per job.
    """
    run_task_name = f"{run_task.__module__}.{run_task.__qualname__}"

    def task_id(job: JobModel) -> int | None:
        return int(job.args[1]) if job.func_name == run_task_name and len(job.args) == 2 else None

    task_ids = {task_id(job) for job in jobs} - {None}
    tasks = Task.objects.prefetch_related("callable_args", "callable_kwargs").in_bulk(task_ids)
    call_strings = {}
    for job in jobs:
        task = tasks.get(task_id(job))
        call_strings[job.name] = task.function_string() if task is not None else job.func_name
    return call_strings


def _enqueue_multiple_jobs(queue: Queue, job_names: list[str], at_front: bool = False) -> int:
    jobs = JobModel.get_many(job_names, connection=queue.connection)
    jobs_requeued = 0
    with queue.connection.pipeline() as pipe:
        for job in jobs:
            if job is None:
                continue
            job.save(connection=pipe)
            queue.enqueue_job(job, pipeline=pipe, at_front=at_front)
            jobs_requeued += 1
        pipe.execute()
    return jobs_requeued
