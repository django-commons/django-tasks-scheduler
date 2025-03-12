import dataclasses
from html import escape
from math import ceil
from typing import Tuple, Optional, List, Union, Dict, Any

from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.http import JsonResponse, HttpResponse, HttpRequest
from django.http.response import HttpResponseNotFound, Http404, HttpResponseBadRequest
from django.shortcuts import redirect
from django.shortcuts import render
from django.urls import reverse, resolve
from django.views.decorators.cache import never_cache

from scheduler.helpers.queues import Queue, InvalidJobOperation
from scheduler.helpers.queues import get_all_workers, get_queue as get_queue_base
from .broker_types import ConnectionErrorTypes, ResponseErrorTypes
from .redis_models import Result, WorkerModel
from .redis_models.job import JobModel
from .redis_models.registry.base_registry import JobNamesRegistry
from .settings import SCHEDULER_CONFIG, logger, get_queue_names, QueueNotFoundError
from .worker.commands import StopJobCommand
from .worker.commands import send_command


def get_queue(queue_name: str) -> Queue:
    try:
        return get_queue_base(queue_name)
    except QueueNotFoundError as e:
        logger.error(e)
        raise Http404(e)


def get_worker_executions(worker: WorkerModel) -> List[JobModel]:
    res = list()
    for queue_name in worker.queue_names:
        queue = get_queue(queue_name)
        curr_jobs = queue.get_all_jobs()
        curr_jobs = [j for j in curr_jobs if j.worker_name == worker.name]
        res.extend(curr_jobs)
    return res


def stats_json(request: HttpRequest) -> Union[JsonResponse, HttpResponseNotFound]:
    auth_token = request.headers.get("Authorization")
    token_validation_func = SCHEDULER_CONFIG.TOKEN_VALIDATION_METHOD
    if request.user.is_staff or (token_validation_func and auth_token and token_validation_func(auth_token)):
        return JsonResponse(get_statistics())

    return HttpResponseNotFound()


# Create your views here.
@never_cache
@staff_member_required
def stats(request: HttpRequest) -> HttpResponse:
    context_data = {**admin.site.each_context(request), **get_statistics(run_maintenance_tasks=True)}
    return render(request, "admin/scheduler/stats.html", context_data)


@dataclasses.dataclass
class QueueData:
    name: str
    jobs: int
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
                jobs=queue.count,
                oldest_job_timestamp=oldest_job_timestamp,
                connection_kwargs=connection_kwargs,
                scheduler_pid=queue.scheduler_pid,
                workers=queue_workers_count[queue.name],
                finished_jobs=len(queue.finished_job_registry),
                started_jobs=len(queue.started_job_registry),
                failed_jobs=len(queue.failed_job_registry),
                scheduled_jobs=len(queue.scheduled_job_registry),
                canceled_jobs=len(queue.canceled_job_registry),
            )
            queues.append(queue_data)
        except ConnectionErrorTypes as e:
            logger.error(f"Could not connect for queue {queue_name}: {e}")
            continue

    return {"queues": [dataclasses.asdict(q) for q in queues]}


def _get_registry_job_list(
        queue: Queue, registry: JobNamesRegistry, page: int
) -> Tuple[List[JobModel], int, range]:
    items_per_page = SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE
    num_jobs = registry.count(queue.connection)
    job_list = list()

    if num_jobs == 0:
        return job_list, num_jobs, range(1, 1)

    last_page = int(ceil(num_jobs / items_per_page))
    page_range = range(1, last_page + 1)
    offset = items_per_page * (page - 1)
    job_ids = registry.all(offset, offset + items_per_page - 1)
    job_list = JobModel.get_many(job_ids, connection=queue.connection)
    remove_job_ids = [job_id for i, job_id in enumerate(job_ids) if job_list[i] is None]
    valid_jobs = [job for job in job_list if job is not None]
    if registry is not queue:
        for job_id in remove_job_ids:
            registry.delete(queue.connection, job_id)

    return valid_jobs, num_jobs, page_range


@never_cache
@staff_member_required
def jobs_view(request: HttpRequest, queue_name: str, registry_name: str) -> HttpResponse:
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
    from .redis_models.worker import WorkerModel

    all_workers = WorkerModel.all(queue.connection)
    worker_list = [worker for worker in all_workers if queue.name in worker.queue_names]

    context_data = {
        **admin.site.each_context(request),
        "queue": queue,
        "workers": worker_list,
    }
    return render(request, "admin/scheduler/queue_workers.html", context_data)


@never_cache
@staff_member_required
def workers(request: HttpRequest) -> HttpResponse:
    all_workers = get_all_workers()
    worker_list = [worker for worker in all_workers]

    context_data = {
        **admin.site.each_context(request),
        "workers": worker_list,
    }
    return render(request, "admin/scheduler/workers.html", context_data)


@never_cache
@staff_member_required
def worker_details(request: HttpRequest, name: str) -> HttpResponse:
    queue_names = get_queue_names()
    queue = get_queue(queue_names[0])
    workers = get_all_workers()
    worker = next((w for w in workers if w.name == name), None)

    if worker is None:
        raise Http404(f"Couldn't find worker with this ID: {name}")

    execution_list = get_worker_executions(worker)
    paginator = Paginator(execution_list, SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE)
    page_number = request.GET.get("p", 1)
    page_obj = paginator.get_page(page_number)
    page_range = paginator.get_elided_page_range(page_obj.number)
    context_data = {
        **admin.site.each_context(request),
        "queue": queue,
        "worker": worker,
        "queue_names": ", ".join(worker.queue_names),
        "job": worker.get_field("current_job_name", queue.connection),
        "total_working_time": worker.total_working_time,
        "executions": page_obj,
        "page_range": page_range,
        "page_var": "p",
    }
    return render(request, "admin/scheduler/worker_details.html", context_data)


def _find_job(job_name: str) -> Tuple[Optional[Queue], Optional[JobModel]]:
    queue_names = get_queue_names()
    for queue_name in queue_names:
        try:
            queue = get_queue(queue_name)
            job = JobModel.get(job_name, connection=queue.connection)
            if job is not None and job.queue_name == queue_name:
                return queue, job
        except Exception as e:
            logger.debug(f"Got exception: {e}")
            pass
    return None, None


@never_cache
@staff_member_required
def job_detail(request: HttpRequest, job_id: str) -> HttpResponse:
    queue, job = _find_job(job_id)
    if job is None:
        return HttpResponseBadRequest(f"Job {escape(job_id)} does not exist, maybe its TTL has passed")
    try:
        job.func_name
        data_is_valid = True
    except Exception:
        data_is_valid = False

    try:
        last_result = Result.fetch_latest(queue.connection, job.name)
        exc_info = last_result.exc_string
    except AttributeError:
        exc_info = None

    context_data = {
        **admin.site.each_context(request),
        "job": job,
        "queue": queue,
        "data_is_valid": data_is_valid,
        "exc_info": exc_info,
    }
    return render(request, "admin/scheduler/job_detail.html", context_data)


@never_cache
@staff_member_required
def clear_queue_registry(request: HttpRequest, queue_name: str, registry_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    registry = queue.get_registry(registry_name)
    if registry is None:
        return HttpResponseNotFound()

    next_url = request.META.get("HTTP_REFERER") or reverse("queue_registry_jobs", args=[queue_name, registry_name])
    if request.method == "POST":
        try:
            if registry is queue:
                queue.empty()
            elif isinstance(registry, JobNamesRegistry):
                job_names = registry.all()
                for job_id in job_names:
                    registry.delete(registry.connection, job_id)
                    job_model = JobModel.get(job_id, connection=registry.connection)
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
        "jobs": [JobModel.get(job_id, connection=queue.connection) for job_id in job_names],
        "next_url": next_url,
        "action_url": reverse("queue_requeue_all", args=[queue_name, registry_name]),
    }

    return render(request, "admin/scheduler/confirm_action.html", context_data)


@never_cache
@staff_member_required
def confirm_action(request: HttpRequest, queue_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    next_url = request.META.get("HTTP_REFERER") or reverse("queue_registry_jobs", args=[queue_name, "queued"])
    try:
        resolve(next_url)
    except Exception:
        messages.warning(request, "Bad followup URL")
        next_url = reverse("queue_registry_jobs", args=[queue_name, "queued"])

    if request.method == "POST" and request.POST.get("action", False):
        # confirm action
        if request.POST.get("_selected_action", False):
            job_id_list = request.POST.getlist("_selected_action")
            context_data = {
                **admin.site.each_context(request),
                "action": request.POST["action"],
                "jobs": [JobModel.get(job_id, connection=queue.connection) for job_id in job_id_list],
                "total_jobs": len(job_id_list),
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

    return redirect(next_url)


@never_cache
@staff_member_required
def actions(request: HttpRequest, queue_name: str) -> HttpResponse:
    queue = get_queue(queue_name)
    next_url = request.POST.get("next_url") or reverse("queue_registry_jobs", args=[queue_name, "queued"])
    try:
        resolve(next_url)
    except Exception:
        messages.warning(request, "Bad followup URL")
        next_url = reverse("queue_registry_jobs", args=[queue_name, "queued"])

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
            # Remove job id from queue and delete the actual job
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


SUPPORTED_JOB_ACTIONS = {"requeue", "delete", "enqueue", "cancel"}


@never_cache
@staff_member_required
def job_action(request: HttpRequest, job_id: str, action: str) -> HttpResponse:
    queue, job = _find_job(job_id)
    if job is None:
        return HttpResponseBadRequest(f"Job {escape(job_id)} does not exist, maybe its TTL has passed")
    if action not in SUPPORTED_JOB_ACTIONS:
        return HttpResponseNotFound()

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
            return redirect("job_details", job_id)
        elif action == "delete":
            # Remove job id from queue and delete the actual job
            queue.delete_job(job.name)
            messages.info(request, f"You have successfully deleted {job.name}")
            return redirect("queue_registry_jobs", queue.name, "queued")
        elif action == "enqueue":
            queue.delete_job(job.name)
            queue._enqueue_job(job)
            messages.info(request, f"You have successfully enqueued {job.name}")
            return redirect("job_details", job_id)
        elif action == "cancel":
            queue.cancel_job(job.name)
            messages.info(request, "You have successfully enqueued %s" % job.name)
            return redirect("job_details", job_id)
    except InvalidJobOperation as e:
        logger.warning(f"Could not perform action: {e}")
        messages.warning(request, f"Could not perform action: {e}")
    return redirect("job_details", job_id)
