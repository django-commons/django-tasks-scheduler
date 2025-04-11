from typing import Optional, List
from typing import Tuple
from urllib.parse import urlparse

from django.contrib import messages
from django.http import Http404
from django.http import HttpRequest
from django.urls import resolve
from django.utils.http import url_has_allowed_host_and_scheme

from scheduler.helpers.queues import Queue
from scheduler.helpers.queues import get_queue as get_queue_base
from scheduler.redis_models import JobModel
from scheduler.settings import QueueNotFoundError
from scheduler.settings import get_queue_names, logger

_QUEUES_WITH_BAD_CONFIGURATION = set()


def get_queue(queue_name: str) -> Queue:
    try:
        return get_queue_base(queue_name)
    except QueueNotFoundError as e:
        logger.error(e)
        raise Http404(e)


def _find_job(job_name: str) -> Tuple[Optional[Queue], Optional[JobModel]]:
    queue_names = get_queue_names()
    for queue_name in queue_names:
        if queue_name in _QUEUES_WITH_BAD_CONFIGURATION:
            continue
        try:
            queue = get_queue(queue_name)
            job = JobModel.get(job_name, connection=queue.connection)
            if job is not None and job.queue_name == queue_name:
                return queue, job
        except Exception as e:
            _QUEUES_WITH_BAD_CONFIGURATION.add(queue_name)
            logger.debug(f"Queue {queue_name} added to bad configuration - Got exception: {e}")
            pass
    return None, None


def _check_next_url(request: HttpRequest, default_next_url: str) -> str:
    next_url = request.POST.get("next_url", default_next_url)
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


def _enqueue_multiple_jobs(queue: Queue, job_names: List[str], at_front: bool = False) -> int:
    jobs = JobModel.get_many(job_names, connection=queue.connection)
    jobs_requeued = 0
    with queue.connection.pipeline() as pipe:
        for job in jobs:
            if job is None:
                continue
            job.save(connection=pipe)
            queue.enqueue_job(job, connection=pipe, at_front=at_front)
            jobs_requeued += 1
        pipe.execute()
    return jobs_requeued
