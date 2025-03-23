from html import escape

from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse, HttpRequest
from django.http.response import HttpResponseBadRequest
from django.shortcuts import render, redirect
from django.views.decorators.cache import never_cache

from scheduler.helpers.queues import InvalidJobOperation
from scheduler.redis_models import Result
from scheduler.settings import logger
from scheduler.views.helpers import _find_job
from scheduler.worker.commands import send_command, StopJobCommand


@never_cache
@staff_member_required
def job_detail(request: HttpRequest, job_name: str) -> HttpResponse:
    queue, job = _find_job(job_name)
    if job is None:
        messages.warning(request, f"Job {escape(job_name)} does not exist, maybe its TTL has passed")
        return redirect("queues_home")
    try:
        job.func_name
        data_is_valid = True
    except Exception:
        data_is_valid = False

    try:
        last_result = Result.fetch_latest(queue.connection, job.name)
    except AttributeError:
        last_result = None

    context_data = {
        **admin.site.each_context(request),
        "job": job,
        "last_result": last_result,
        "results": Result.all(connection=queue.connection, parent=job.name),
        "queue": queue,
        "data_is_valid": data_is_valid,
    }
    return render(request, "admin/scheduler/job_detail.html", context_data)


SUPPORTED_JOB_ACTIONS = {"requeue", "delete", "enqueue", "cancel"}


@never_cache
@staff_member_required
def job_action(request: HttpRequest, job_name: str, action: str) -> HttpResponse:
    queue, job = _find_job(job_name)
    if job is None:
        messages.warning(request, f"Job {escape(job_name)} does not exist, maybe its TTL has passed")
        return redirect("queues_home")
    if action not in SUPPORTED_JOB_ACTIONS:
        return HttpResponseBadRequest(f"Action {escape(action)} is not supported")

    if request.method != "POST":
        context_data = {
            **admin.site.each_context(request),
            "job": job,
            "queue": queue,
            "action": action,
        }
        return render(request, "admin/scheduler/single_job_action.html", context_data)

    try:
        if action == "requeue":
            requeued_jobs_count = queue.requeue_jobs(job.name)
            if requeued_jobs_count == 0:
                messages.warning(request, f"Could not requeue {job.name}")
            else:
                messages.info(request, f"You have successfully re-queued {job.name}")
            return redirect("job_details", job_name)
        elif action == "delete":
            queue.delete_job(job.name)
            messages.info(request, f"You have successfully deleted {job.name}")
            return redirect("queue_registry_jobs", queue.name, "queued")
        elif action == "enqueue":
            queue.delete_job(job.name, expire_job_model=False)
            queue.enqueue_job(job)
            messages.info(request, f"You have successfully enqueued {job.name}")
            return redirect("job_details", job_name)
        elif action == "cancel":
            send_command(
                connection=queue.connection, command=StopJobCommand(job_name=job.name, worker_name=job.worker_name)
            )
            queue.cancel_job(job.name)
            messages.info(request, f"You have successfully cancelled {job.name}")
            return redirect("job_details", job_name)
    except InvalidJobOperation as e:
        logger.warning(f"Could not perform action: {e}")
        messages.warning(request, f"Could not perform action: {e}")
    return redirect("job_details", job_name)
