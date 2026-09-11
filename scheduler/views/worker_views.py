from collections import defaultdict

from django.contrib import admin
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import render
from django.views.decorators.cache import never_cache

from scheduler.helpers.queues import get_all_workers
from scheduler.helpers.queues.getters import get_worker
from scheduler.models import Task
from scheduler.redis_models import JobModel, Result, WorkerModel, as_str
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.views.helpers import get_queue


def _worker_job_names(worker: WorkerModel) -> list[tuple[str, str]]:
    """Returns (queue name, job name) for the jobs the worker ran, reading only each job's worker name - not the whole
    job - to find them."""
    entries = []
    for queue_name in worker.queue_names:
        queue = get_queue(queue_name)
        job_names = queue.get_all_job_names()
        with queue.connection.pipeline() as pipeline:
            for job_name in job_names:
                pipeline.hget(JobModel.key_for(job_name), "worker_name")
            worker_names = pipeline.execute()
        entries.extend(
            (queue_name, job_name)
            for job_name, worker_name in zip(job_names, worker_names)
            if worker_name is not None and as_str(worker_name) == worker.name
        )
    return entries


def _get_jobs(entries: list[tuple[str, str]]) -> list[JobModel]:
    """Returns the jobs `entries` name, in order, with one round trip per queue."""
    job_names_by_queue: dict[str, list[str]] = defaultdict(list)
    for queue_name, job_name in entries:
        job_names_by_queue[queue_name].append(job_name)
    jobs: dict[tuple[str, str], JobModel] = {}
    for queue_name, job_names in job_names_by_queue.items():
        for job in JobModel.get_many(job_names, connection=get_queue(queue_name).connection):
            if job is not None:
                jobs[(queue_name, job.name)] = job
    return [jobs[entry] for entry in entries if entry in jobs]


def _latest_results(jobs: list[JobModel]) -> dict[str, Result]:
    """Returns the jobs' latest results by job name, in one round trip per queue."""
    job_names_by_queue: dict[str, list[str]] = defaultdict(list)
    for job in jobs:
        job_names_by_queue[job.queue_name].append(job.name)
    latest_results: dict[str, Result] = {}
    for queue_name, job_names in job_names_by_queue.items():
        latest_results.update(Result.fetch_latest_many(get_queue(queue_name).connection, job_names))
    return latest_results


@never_cache  # type: ignore
@staff_member_required  # type: ignore
def worker_details(request: HttpRequest, name: str) -> HttpResponse:
    worker = get_worker(name)

    if worker is None:
        raise Http404(f"Couldn't find worker with this ID: {name}")

    # Paginate the job names, and fetch whole jobs for the current page only.
    paginator = Paginator(_worker_job_names(worker), SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE)
    page_number = request.GET.get("p", 1)
    page_obj = paginator.get_page(page_number)
    page_obj.object_list = _get_jobs(list(page_obj.object_list))
    page_range = paginator.get_elided_page_range(page_obj.number)
    current_job = None
    if worker.current_job_name is not None:
        queue = get_queue(worker.queue_names[0])
        current_job = JobModel.get(worker.current_job_name, connection=queue.connection)
    page_jobs = list(page_obj)
    task_ids = {job.scheduled_task_id for job in page_jobs if job.scheduled_task_id is not None}
    context_data = {
        **admin.site.each_context(request),
        "worker": worker,
        "queue_names": ", ".join(worker.queue_names),
        "current_job": current_job,
        "executions": page_obj,
        "latest_results": _latest_results(page_jobs),
        "task_names": dict(Task.objects.filter(id__in=task_ids).values_list("id", "name")),
        "page_range": page_range,
        "page_var": "p",
    }
    return render(request, "admin/scheduler/worker_details.html", context_data)


@never_cache  # type: ignore
@staff_member_required  # type: ignore
def workers_list(request: HttpRequest) -> HttpResponse:
    all_workers = get_all_workers()
    worker_list = list(all_workers)

    context_data = {
        **admin.site.each_context(request),
        "workers": worker_list,
    }
    return render(request, "admin/scheduler/workers_list.html", context_data)
