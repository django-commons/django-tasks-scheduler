"""list_registry_jobs actions on all jobs in the registry"""

from enum import Enum

from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse, HttpRequest, HttpResponseNotFound
from django.shortcuts import render, redirect
from django.urls import reverse
from django.views.decorators.cache import never_cache

from scheduler.helpers.queues import Queue
from scheduler.redis_models import JobModel, JobNamesRegistry
from scheduler.types import ResponseErrorTypes
from scheduler.views.helpers import get_queue, _check_next_url, _enqueue_multiple_jobs


class QueueRegistryActions(Enum):
    EMPTY = "empty"
    REQUEUE = "requeue"


def _clear_registry(request: HttpRequest, queue: Queue, registry_name: str, registry: JobNamesRegistry):
    try:
        job_names = registry.all()
        for job_name in job_names:
            registry.delete(registry.connection, job_name)
            job_model = JobModel.get(job_name, connection=registry.connection)
            job_model.delete(connection=registry.connection)
        messages.info(request, f"You have successfully cleared the {registry_name} jobs in queue {queue.name}")
    except ResponseErrorTypes as e:
        messages.error(request, f"error: {e}")
        raise e


def _requeue_job_names(request: HttpRequest, queue: Queue, registry_name: str):
    registry = queue.get_registry(registry_name)
    job_names = registry.all()
    jobs_requeued_count = _enqueue_multiple_jobs(queue, job_names)
    messages.info(request, f"You have successfully re-queued {jobs_requeued_count} jobs!")


@never_cache
@staff_member_required
def queue_registry_actions(request: HttpRequest, queue_name: str, registry_name: str, action: str) -> HttpResponse:
    queue = get_queue(queue_name)
    registry = queue.get_registry(registry_name)
    if registry is None:
        return HttpResponseNotFound()
    next_url = _check_next_url(request, reverse("queue_registry_jobs", args=[queue_name, registry_name]))
    if action not in [item.value for item in QueueRegistryActions]:
        return redirect(next_url)
    if request.method == "POST":
        if action == QueueRegistryActions.EMPTY.value:
            _clear_registry(request, queue, registry_name, registry)
        elif action == QueueRegistryActions.REQUEUE.value:
            _requeue_job_names(request, queue, registry_name)
        return redirect("queue_registry_jobs", queue_name, registry_name)
    job_names = registry.all()
    job_list = JobModel.get_many(job_names, connection=queue.connection)
    context_data = {
        **admin.site.each_context(request),
        "queue": queue,
        "total_jobs": len(registry),
        "action": action,
        "jobs": job_list,
        "next_url": next_url,
        "action_url": reverse("queue_registry_action", args=[queue_name, registry_name, action]),
    }
    return render(request, "admin/scheduler/confirm_action.html", context_data)
