from collections import defaultdict

from django.contrib import admin
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import render
from django.views.decorators.cache import never_cache

from scheduler.helpers.queues import get_all_workers
from scheduler.models import Task
from scheduler.redis_models import JobModel, Result, WorkerModel
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.views.helpers import get_queue


def get_worker_executions(worker: WorkerModel) -> list[JobModel]:
    res = []
    for queue_name in worker.queue_names:
        queue = get_queue(queue_name)
        curr_jobs = queue.get_all_jobs()
        curr_jobs = [j for j in curr_jobs if j.worker_name == worker.name]
        res.extend(curr_jobs)
    return res


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
    workers = get_all_workers()
    worker = next((w for w in workers if w.name == name), None)

    if worker is None:
        raise Http404(f"Couldn't find worker with this ID: {name}")

    execution_list = get_worker_executions(worker)
    paginator = Paginator(execution_list, SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE)
    page_number = request.GET.get("p", 1)
    page_obj = paginator.get_page(page_number)
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
