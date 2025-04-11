"""list_registry_jobs actions on multiple selected jobs"""

from enum import Enum

from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse, HttpRequest
from django.shortcuts import render, redirect
from django.urls import reverse
from django.views.decorators.cache import never_cache

from scheduler.redis_models import JobModel
from scheduler.settings import logger
from scheduler.views.helpers import get_queue, _check_next_url, _enqueue_multiple_jobs
from scheduler.worker.commands import StopJobCommand, send_command


class QueueJobAction(Enum):
    DELETE = "delete"
    REQUEUE = "requeue"
    STOP = "stop"

    def __contains__(self, item) -> bool:
        return item in [a.value for a in self.__class__]


@never_cache
@staff_member_required
def queue_job_actions(request: HttpRequest, queue_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    next_url = _check_next_url(request, reverse("queue_registry_jobs", args=[queue_name, "queued"]))
    action = request.POST.get("action", False)
    job_names = request.POST.get("job_names", False)
    if request.method != "POST" or not action or not job_names or action not in [item.value for item in QueueJobAction]:
        return redirect(next_url)
    job_names = request.POST.getlist("job_names")
    if action == QueueJobAction.DELETE.value:
        jobs = JobModel.get_many(job_names, connection=queue.connection)
        for job in jobs:
            if job is None:
                continue
            queue.delete_job(job.name)
        messages.info(request, f"You have successfully deleted {len(job_names)} jobs!")
    elif action == QueueJobAction.REQUEUE.value:
        requeued_jobs_count = _enqueue_multiple_jobs(queue, job_names)
        messages.info(request, f"You have successfully re-queued {requeued_jobs_count}/{len(job_names)}  jobs!")
    elif action == QueueJobAction.STOP.value:
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


@never_cache
@staff_member_required
def queue_confirm_job_action(request: HttpRequest, queue_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    next_url = _check_next_url(request, reverse("queue_registry_jobs", args=[queue_name, "queued"]))
    action = request.POST.get("action", None)
    job_names = request.POST.getlist("_selected_action", None)
    if request.method != "POST" or action is None or job_names is None or action not in QueueJobAction:
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
            "queue_job_actions",
            args=[
                queue_name,
            ],
        ),
    }
    return render(request, "admin/scheduler/confirm_action.html", context_data)
