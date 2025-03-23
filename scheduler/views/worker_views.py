from typing import List

from django.contrib import admin
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.http import HttpResponse, HttpRequest, Http404
from django.shortcuts import render
from django.views.decorators.cache import never_cache

from scheduler.helpers.queues import get_all_workers
from scheduler.redis_models import WorkerModel, JobModel
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.views.helpers import get_queue


def get_worker_executions(worker: WorkerModel) -> List[JobModel]:
    res = list()
    for queue_name in worker.queue_names:
        queue = get_queue(queue_name)
        curr_jobs = queue.get_all_jobs()
        curr_jobs = [j for j in curr_jobs if j.worker_name == worker.name]
        res.extend(curr_jobs)
    return res


@never_cache
@staff_member_required
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
    context_data = {
        **admin.site.each_context(request),
        "worker": worker,
        "queue_names": ", ".join(worker.queue_names),
        "current_job": current_job,
        "executions": page_obj,
        "page_range": page_range,
        "page_var": "p",
    }
    return render(request, "admin/scheduler/worker_details.html", context_data)


@never_cache
@staff_member_required
def workers_list(request: HttpRequest) -> HttpResponse:
    all_workers = get_all_workers()
    worker_list = [worker for worker in all_workers]

    context_data = {
        **admin.site.each_context(request),
        "workers": worker_list,
    }
    return render(request, "admin/scheduler/workers_list.html", context_data)
