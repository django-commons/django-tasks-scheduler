import dataclasses
from math import ceil
from typing import Tuple, List, Dict, Union, Any, Optional

from django.contrib import admin
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse, HttpRequest, HttpResponseNotFound, JsonResponse
from django.shortcuts import render
from django.views.decorators.cache import never_cache

from scheduler import settings
from scheduler.helpers.queues import Queue, get_all_workers
from scheduler.redis_models import JobModel, JobNamesRegistry, WorkerModel
from scheduler.settings import get_queue_names, logger
from scheduler.types import ConnectionErrorTypes
from scheduler.views.helpers import get_queue


def _get_registry_job_list(queue: Queue, registry: JobNamesRegistry, page: int) -> Tuple[List[JobModel], int, range]:
    items_per_page = settings.SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE
    num_jobs = registry.count(queue.connection)
    job_list = list()

    if num_jobs == 0:
        return job_list, num_jobs, range(1, 1)

    last_page = int(ceil(num_jobs / items_per_page))
    page_range = range(1, last_page + 1)
    offset = items_per_page * (page - 1)
    job_names = registry.all(offset, offset + items_per_page - 1)
    job_list = JobModel.get_many(job_names, connection=queue.connection)
    remove_job_names = [job_name for i, job_name in enumerate(job_names) if job_list[i] is None]
    valid_jobs = [job for job in job_list if job is not None]
    if registry is not queue:
        for job_name in remove_job_names:
            registry.delete(queue.connection, job_name)

    return valid_jobs, num_jobs, page_range


@never_cache
@staff_member_required
def list_registry_jobs(request: HttpRequest, queue_name: str, registry_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    registry = queue.get_registry(registry_name)
    if registry is None:
        return HttpResponseNotFound()
    title = registry_name.capitalize()
    page = int(request.GET.get("page", 1))
    job_list, num_jobs, page_range = _get_registry_job_list(queue, registry, page)

    context_data = {
        **admin.site.each_context(request),
        "queue": queue,
        "registry_name": registry_name,
        "registry": registry,
        "jobs": job_list,
        "num_jobs": num_jobs,
        "page": page,
        "page_range": page_range,
        "job_status": title,
    }
    return render(request, "admin/scheduler/jobs.html", context_data)


@never_cache
@staff_member_required
def queue_workers(request: HttpRequest, queue_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    queue.clean_registries()

    all_workers = WorkerModel.all(queue.connection)
    worker_list = [worker for worker in all_workers if queue.name in worker.queue_names]

    context_data = {
        **admin.site.each_context(request),
        "queue": queue,
        "workers": worker_list,
    }
    return render(request, "admin/scheduler/queue_workers.html", context_data)


def stats_json(request: HttpRequest) -> Union[JsonResponse, HttpResponseNotFound]:
    auth_token = request.headers.get("Authorization")
    token_validation_func = settings.SCHEDULER_CONFIG.TOKEN_VALIDATION_METHOD
    if request.user.is_staff or (token_validation_func and auth_token and token_validation_func(auth_token)):
        return JsonResponse(get_statistics())

    return HttpResponseNotFound()


@never_cache
@staff_member_required
def stats(request: HttpRequest) -> HttpResponse:
    context_data = {**admin.site.each_context(request), **get_statistics(run_maintenance_tasks=True)}
    return render(request, "admin/scheduler/stats.html", context_data)


@dataclasses.dataclass
class QueueData:
    name: str
    queued_jobs: int
    oldest_job_timestamp: Optional[str]
    scheduler_pid: Optional[int]
    workers: int
    finished_jobs: int
    started_jobs: int
    failed_jobs: int
    scheduled_jobs: int
    canceled_jobs: int


def get_statistics(run_maintenance_tasks: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    queue_names = get_queue_names()
    queues: List[QueueData] = []
    queue_workers_count: Dict[str, int] = {queue_name: 0 for queue_name in queue_names}
    workers = get_all_workers()
    for worker in workers:
        for queue_name in worker.queue_names:
            if queue_name not in queue_workers_count:
                logger.warning(f"Worker {worker.name} is working on queue {queue_name} which is not defined")
                queue_workers_count[queue_name] = 0
            queue_workers_count[queue_name] += 1
    for queue_name in queue_names:
        try:
            queue = get_queue(queue_name)
            connection_kwargs = queue.connection.connection_pool.connection_kwargs

            if run_maintenance_tasks:
                queue.clean_registries()

            # Raw access to the first item from left of the broker list.
            # This might not be accurate since new job can be added from the left
            # with `at_front` parameters.
            # Ideally rq should supports Queue.oldest_job

            last_job_name = queue.first_queued_job_name()
            last_job = JobModel.get(last_job_name, connection=queue.connection) if last_job_name else None
            if last_job and last_job.enqueued_at:
                oldest_job_timestamp = last_job.enqueued_at.isoformat()
            else:
                oldest_job_timestamp = None

            # parse_class and connection_pool are not needed and not JSON serializable
            connection_kwargs.pop("parser_class", None)
            connection_kwargs.pop("connection_pool", None)

            queue_data = QueueData(
                name=queue.name,
                queued_jobs=len(queue.queued_job_registry),
                oldest_job_timestamp=oldest_job_timestamp,
                scheduler_pid=queue.scheduler_pid,
                workers=queue_workers_count[queue.name],
                finished_jobs=len(queue.finished_job_registry),
                started_jobs=len(queue.active_job_registry),
                failed_jobs=len(queue.failed_job_registry),
                scheduled_jobs=len(queue.scheduled_job_registry),
                canceled_jobs=len(queue.canceled_job_registry),
            )
            queues.append(queue_data)
        except ConnectionErrorTypes as e:
            logger.error(f"Could not connect for queue {queue_name}: {e}")
            continue

    return {"queues": [dataclasses.asdict(q) for q in queues]}
