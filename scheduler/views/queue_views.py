import dataclasses
from math import ceil
from typing import Tuple, List, Dict, Union, Any
from urllib.parse import urlparse

from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse, HttpRequest, HttpResponseNotFound, JsonResponse
from django.shortcuts import render, redirect
from django.urls import reverse, resolve
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.cache import never_cache

from scheduler.broker_types import ConnectionErrorTypes, ResponseErrorTypes
from scheduler.helpers.queues import Queue, get_all_workers
from scheduler.redis_models import JobModel, JobNamesRegistry, WorkerModel
from scheduler.settings import SCHEDULER_CONFIG, get_queue_names, logger
from scheduler.views.helpers import get_queue
from scheduler.worker.commands import StopJobCommand, send_command


def _get_registry_job_list(queue: Queue, registry: JobNamesRegistry, page: int) -> Tuple[List[JobModel], int, range]:
    items_per_page = SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE
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
def registry_jobs(request: HttpRequest, queue_name: str, registry_name: str) -> HttpResponse:
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
    token_validation_func = SCHEDULER_CONFIG.TOKEN_VALIDATION_METHOD
    if request.user.is_staff or (token_validation_func and auth_token and token_validation_func(auth_token)):
        return JsonResponse(get_statistics())

    return HttpResponseNotFound()


@never_cache
@staff_member_required
def stats(request: HttpRequest) -> HttpResponse:
    context_data = {**admin.site.each_context(request), **get_statistics(run_maintenance_tasks=True)}
    return render(request, "admin/scheduler/stats.html", context_data)


@never_cache
@staff_member_required
def clear_queue_registry(request: HttpRequest, queue_name: str, registry_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    registry = queue.get_registry(registry_name)
    if registry is None:
        return HttpResponseNotFound()
    next_url = _check_next_url(request, reverse("queue_registry_jobs", args=[queue_name, registry_name]))
    if request.method == "POST":
        try:
            if registry is queue:
                queue.empty()
            elif isinstance(registry, JobNamesRegistry):
                job_names = registry.all()
                for job_name in job_names:
                    registry.delete(registry.connection, job_name)
                    job_model = JobModel.get(job_name, connection=registry.connection)
                    job_model.delete(connection=registry.connection)
            messages.info(request, f"You have successfully cleared the {registry_name} jobs in queue {queue.name}")
        except ResponseErrorTypes as e:
            messages.error(request, f"error: {e}")
            raise e
        return redirect("queue_registry_jobs", queue_name, registry_name)
    job_names = registry.all()
    job_list = JobModel.get_many(job_names, connection=queue.connection)
    context_data = {
        **admin.site.each_context(request),
        "queue": queue,
        "total_jobs": len(registry),
        "action": "empty",
        "jobs": job_list,
        "next_url": next_url,
        "action_url": reverse(
            "queue_clear",
            args=[
                queue_name,
                registry_name,
            ],
        ),
    }
    return render(request, "admin/scheduler/confirm_action.html", context_data)


@never_cache
@staff_member_required
def requeue_all(request: HttpRequest, queue_name: str, registry_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    registry = queue.get_registry(registry_name)
    if registry is None:
        return HttpResponseNotFound()
    next_url = request.META.get("HTTP_REFERER") or reverse("queue_registry_jobs", args=[queue_name, registry_name])
    job_names = registry.all()
    if request.method == "POST":
        # Confirmation received
        jobs_requeued_count = queue.requeue_jobs(*job_names)
        messages.info(request, f"You have successfully re-queued {jobs_requeued_count} jobs!")
        return redirect("queue_registry_jobs", queue_name, registry_name)

    context_data = {
        **admin.site.each_context(request),
        "queue": queue,
        "total_jobs": queue.count,
        "action": "requeue",
        "jobs": [JobModel.get(job_name, connection=queue.connection) for job_name in job_names],
        "next_url": next_url,
        "action_url": reverse("queue_requeue_all", args=[queue_name, registry_name]),
    }

    return render(request, "admin/scheduler/confirm_action.html", context_data)


@never_cache
@staff_member_required
def queue_confirm_action(request: HttpRequest, queue_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    next_url = _check_next_url(request, reverse("queue_registry_jobs", args=[queue_name, "queued"]))
    if request.method != "POST":
        return redirect(next_url)
    action = request.POST.get("action", None)
    job_names = request.POST.getlist("_selected_action", None)
    if action is None or job_names is None:
        return redirect(next_url)

    # confirm action
    context_data = {
        **admin.site.each_context(request),
        "action": action,
        "jobs": [JobModel.get(job_name, connection=queue.connection) for job_name in job_names],
        "total_jobs": len(job_names),
        "queue": queue,
        "next_url": next_url,
        "action_url": reverse(
            "queue_actions",
            args=[
                queue_name,
            ],
        ),
    }
    return render(request, "admin/scheduler/confirm_action.html", context_data)


_QUEUE_ACTIONS = {"delete", "requeue", "stop"}


@never_cache
@staff_member_required
def queue_actions(request: HttpRequest, queue_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    next_url = _check_next_url(request, reverse("queue_registry_jobs", args=[queue_name, "queued"]))
    action = request.POST.get("action", False)
    job_names = request.POST.get("job_names", False)
    if request.method != "POST" or not action or not job_names:
        return redirect(next_url)
    job_names = request.POST.getlist("job_names")
    if action == "delete":
        jobs = JobModel.get_many(job_names, connection=queue.connection)
        for job in jobs:
            if job is None:
                continue
            queue.delete_job(job.name)
        messages.info(request, f"You have successfully deleted {len(job_names)} jobs!")
    elif action == "requeue":
        requeued_jobs_count = queue.requeue_jobs(*job_names)
        messages.info(request, f"You have successfully re-queued {requeued_jobs_count}/{len(job_names)}  jobs!")
    elif action == "stop":
        cancelled_jobs = 0
        jobs = JobModel.get_many(job_names, connection=queue.connection)
        for job in jobs:
            if job is None:
                continue
            try:
                command = StopJobCommand(job_name=job.name, worker_name=job.worker_name)
                send_command(connection=queue.connection, command=command)
                queue.cancel_job(job.name)
                cancelled_jobs += 1
            except Exception as e:
                logger.warning(f"Could not stop job: {e}")
                pass
        messages.info(request, f"You have successfully stopped {cancelled_jobs}  jobs!")
    return redirect(next_url)


@dataclasses.dataclass
class QueueData:
    name: str
    queued_jobs: int
    oldest_job_timestamp: str
    connection_kwargs: dict
    scheduler_pid: int
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
                logger.warning(f"Worker {worker.name} ({queue_name}) has no queue")
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
                oldest_job_timestamp = "-"

            # parse_class and connection_pool are not needed and not JSON serializable
            connection_kwargs.pop("parser_class", None)
            connection_kwargs.pop("connection_pool", None)

            queue_data = QueueData(
                name=queue.name,
                queued_jobs=len(queue.queued_job_registry),
                oldest_job_timestamp=oldest_job_timestamp,
                connection_kwargs=connection_kwargs,
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
