from html import escape
from math import ceil
from typing import Tuple, Optional

import redis
from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.http import JsonResponse
from django.http.response import HttpResponseNotFound, Http404, HttpResponseBadRequest
from django.shortcuts import redirect
from django.shortcuts import render
from django.urls import reverse, resolve
from django.views.decorators.cache import never_cache
from redis.exceptions import ResponseError

from .queues import get_all_workers, get_connection, logger, QueueNotFoundError
from .queues import get_queue as get_queue_base
from .rq_classes import JobExecution, DjangoWorker, DjangoQueue, InvalidJobOperation
from .settings import SCHEDULER_CONFIG


def get_queue(queue_name: str) -> DjangoQueue:
    try:
        return get_queue_base(queue_name)
    except QueueNotFoundError as e:
        logger.error(e)
        raise Http404(e)


def get_worker_executions(worker):
    res = list()
    for queue in worker.queues:
        curr_jobs = queue.get_all_jobs()
        curr_jobs = [j for j in curr_jobs if j.worker_name == worker.name]
        res.extend(curr_jobs)
    return res


# Create your views here.
@never_cache
@staff_member_required
def stats(request):
    context_data = {**admin.site.each_context(request), **get_statistics(run_maintenance_tasks=True)}
    return render(request, 'admin/scheduler/stats.html', context_data)


def stats_json(request):
    # TODO support API token
    if request.user.is_staff:
        return JsonResponse(get_statistics())

    return HttpResponseNotFound()


def get_statistics(run_maintenance_tasks=False):
    from scheduler.settings import QUEUES
    queues = []
    if run_maintenance_tasks:
        workers = get_all_workers()
        for worker in workers:
            worker.clean_registries()
    for queue_name in QUEUES:
        try:
            queue = get_queue(queue_name)
            connection = get_connection(QUEUES[queue_name])
            connection_kwargs = connection.connection_pool.connection_kwargs

            if run_maintenance_tasks:
                queue.clean_registries()

            # Raw access to the first item from left of the redis list.
            # This might not be accurate since new job can be added from the left
            # with `at_front` parameters.
            # Ideally rq should supports Queue.oldest_job

            last_job_id = queue.last_job_id()
            last_job = queue.fetch_job(last_job_id.decode('utf-8')) if last_job_id else None
            if last_job and last_job.enqueued_at:
                oldest_job_timestamp = last_job.enqueued_at.strftime('%Y-%m-%d, %H:%M:%S')
            else:
                oldest_job_timestamp = "-"

            # parse_class and connection_pool are not needed and not JSON serializable
            connection_kwargs.pop('parser_class', None)
            connection_kwargs.pop('connection_pool', None)

            queue_data = dict(
                name=queue.name,
                jobs=queue.count,
                oldest_job_timestamp=oldest_job_timestamp,
                connection_kwargs=connection_kwargs,
                scheduler_pid=queue.scheduler_pid,
                workers=DjangoWorker.count(queue=queue),
                finished_jobs=len(queue.finished_job_registry),
                started_jobs=len(queue.started_job_registry),
                deferred_jobs=len(queue.deferred_job_registry),
                failed_jobs=len(queue.failed_job_registry),
                scheduled_jobs=len(queue.scheduled_job_registry),
                canceled_jobs=len(queue.canceled_job_registry),
            )
            queues.append(queue_data)
        except redis.ConnectionError as e:
            logger.error(f'Could not connect for queue {queue_name}: {e}')
            continue

    return {'queues': queues}


def _get_registry_job_list(queue, registry, page):
    items_per_page = SCHEDULER_CONFIG['EXECUTIONS_IN_PAGE']
    num_jobs = len(registry)
    job_list = []

    if num_jobs == 0:
        return job_list, num_jobs, []

    last_page = int(ceil(num_jobs / items_per_page))
    page_range = range(1, last_page + 1)
    offset = items_per_page * (page - 1)
    job_ids = registry.get_job_ids(offset, offset + items_per_page - 1)
    job_list = JobExecution.fetch_many(job_ids, connection=queue.connection)
    remove_job_ids = [job_id for i, job_id in enumerate(job_ids) if job_list[i] is None]
    valid_jobs = [job for job in job_list if job is not None]
    if registry is not queue:
        for job_id in remove_job_ids:
            registry.remove(job_id)

    return valid_jobs, num_jobs, page_range


@never_cache
@staff_member_required
def jobs_view(request, queue_name: str, registry_name: str):
    queue = get_queue(queue_name)
    registry = queue.get_registry(registry_name)
    if registry is None:
        return HttpResponseNotFound()
    title = registry_name.capitalize()
    page = int(request.GET.get('page', 1))
    job_list, num_jobs, page_range = _get_registry_job_list(queue, registry, page)

    context_data = {
        **admin.site.each_context(request),
        'queue': queue,
        'registry_name': registry_name,
        'registry': registry,
        'jobs': job_list,
        'num_jobs': num_jobs,
        'page': page,
        'page_range': page_range,
        'job_status': title,
    }
    return render(request, 'admin/scheduler/jobs.html', context_data)


@never_cache
@staff_member_required
def queue_workers(request, queue_name):
    queue = get_queue(queue_name)
    all_workers = DjangoWorker.all(queue.connection)
    for w in all_workers:
        w.clean_registries()
    worker_list = [worker for worker in all_workers if queue.name in worker.queue_names()]

    context_data = {
        **admin.site.each_context(request),
        'queue': queue,
        'workers': worker_list,
    }
    return render(request, 'admin/scheduler/queue_workers.html', context_data)


@never_cache
@staff_member_required
def workers(request):
    all_workers = get_all_workers()
    worker_list = [worker for worker in all_workers]

    context_data = {
        **admin.site.each_context(request),
        'workers': worker_list,
    }
    return render(request, 'admin/scheduler/workers.html', context_data)


@never_cache
@staff_member_required
def worker_details(request, name):
    queue, worker = None, None
    workers = get_all_workers()
    worker = next((w for w in workers if w.name == name), None)

    if worker is None:
        raise Http404(f"Couldn't find worker with this ID: {name}")
    # Convert microseconds to milliseconds
    worker.total_working_time = worker.total_working_time / 1000

    execution_list = get_worker_executions(worker)
    paginator = Paginator(execution_list, SCHEDULER_CONFIG['EXECUTIONS_IN_PAGE'])
    page_number = request.GET.get('p', 1)
    page_obj = paginator.get_page(page_number)
    page_range = paginator.get_elided_page_range(page_obj.number)
    context_data = {
        **admin.site.each_context(request),
        'queue': queue,
        'worker': worker,
        'queue_names': ', '.join(worker.queue_names()),
        'job': worker.get_current_job(),
        'total_working_time': worker.total_working_time * 1000,
        'executions': page_obj,
        'page_range': page_range,
        'page_var': 'p',
    }
    return render(request, 'admin/scheduler/worker_details.html', context_data)


def _find_job(job_id: str) -> Tuple[Optional[DjangoQueue], Optional[JobExecution]]:
    from scheduler.settings import QUEUES

    for queue_name in QUEUES:
        try:
            queue = get_queue(queue_name)
            job = JobExecution.fetch(job_id, connection=queue.connection)
            if job.origin == queue_name:
                return queue, job
        except Exception:
            pass
    return None, None


@never_cache
@staff_member_required
def job_detail(request, job_id: str):
    queue, job = _find_job(job_id)
    if job is None:
        return HttpResponseBadRequest(f'Job {escape(job_id)} does not exist, maybe its TTL has passed')
    try:
        job.func_name
        data_is_valid = True
    except Exception:
        data_is_valid = False

    try:
        exc_info = job._exc_info
    except AttributeError:
        exc_info = None

    context_data = {
        **admin.site.each_context(request),
        'job': job,
        'dependency_id': job._dependency_id,
        'queue': queue,
        'data_is_valid': data_is_valid,
        'exc_info': exc_info,
    }
    return render(request, 'admin/scheduler/job_detail.html', context_data)


@never_cache
@staff_member_required
def clear_queue_registry(request, queue_name, registry_name):
    queue = get_queue(queue_name)
    registry = queue.get_registry(registry_name)
    if registry is None:
        return HttpResponseNotFound()

    next_url = request.META.get('HTTP_REFERER') or reverse('queue_registry_jobs', args=[queue_name, registry_name])
    if request.method == 'POST':
        try:
            if registry is queue:
                queue.empty()
            else:
                job_ids = registry.get_job_ids()
                for job_id in job_ids:
                    registry.remove(job_id, delete_job=True)
            messages.info(request, f'You have successfully cleared the {registry_name} jobs in queue {queue.name}')
        except ResponseError as e:
            messages.error(request, f'error: {e}', )
            raise e
        return redirect('queue_registry_jobs', queue_name, registry_name)
    job_ids = registry.get_job_ids()
    job_list = JobExecution.fetch_many(job_ids, connection=queue.connection)
    context_data = {
        **admin.site.each_context(request),
        'queue': queue,
        'total_jobs': len(registry),
        'action': 'empty',
        'jobs': job_list,
        'next_url': next_url,
        'action_url': reverse('queue_clear', args=[queue_name, registry_name, ])
    }
    return render(request, 'admin/scheduler/confirm_action.html', context_data)


@never_cache
@staff_member_required
def requeue_all(request, queue_name, registry_name):
    queue = get_queue(queue_name)
    registry = queue.get_registry(registry_name)
    if registry is None:
        return HttpResponseNotFound()
    next_url = request.META.get('HTTP_REFERER') or reverse('queue_registry_jobs', args=[queue_name, registry_name])
    job_ids = registry.get_job_ids()
    if request.method == 'POST':
        count = 0
        # Confirmation received
        for job_id in job_ids:
            try:
                job = JobExecution.fetch(job_id, connection=queue.connection)
                job.requeue()
                count += 1
            except Exception:
                pass

        messages.info(request, f'You have successfully re-queued {count} jobs!')
        return redirect('queue_registry_jobs', queue_name, registry_name)

    context_data = {
        **admin.site.each_context(request),
        'queue': queue,
        'total_jobs': len(queue.failed_job_registry),
        'action': 'requeue',
        'jobs': [queue.fetch_job(job_id) for job_id in job_ids],
        'next_url': next_url,
        'action_url': reverse('queue_requeue_all', args=[queue_name, registry_name])
    }

    return render(request, 'admin/scheduler/confirm_action.html', context_data)


@never_cache
@staff_member_required
def confirm_action(request, queue_name):
    queue = get_queue(queue_name)
    next_url = request.META.get('HTTP_REFERER') or reverse('queue_registry_jobs', args=[queue_name, 'queued'])
    try:
        resolve(next_url)
    except Exception:
        messages.warning(request, 'Bad followup URL')
        next_url = reverse('queue_registry_jobs', args=[queue_name, 'queued'])

    if request.method == 'POST' and request.POST.get('action', False):
        # confirm action
        if request.POST.get('_selected_action', False):
            job_id_list = request.POST.getlist('_selected_action')
            context_data = {
                **admin.site.each_context(request),
                'action': request.POST['action'],
                'jobs': [queue.fetch_job(job_id) for job_id in job_id_list],
                'total_jobs': len(job_id_list),
                'queue': queue,
                'next_url': next_url,
                'action_url': reverse('queue_actions', args=[queue_name, ]),
            }
            return render(request, 'admin/scheduler/confirm_action.html', context_data)

    return redirect(next_url)


@never_cache
@staff_member_required
def actions(request, queue_name):
    queue = get_queue(queue_name)
    next_url = request.POST.get('next_url') or reverse('queue_registry_jobs', args=[queue_name, 'queued'])
    try:
        resolve(next_url)
    except Exception:
        messages.warning(request, 'Bad followup URL')
        next_url = reverse('queue_registry_jobs', args=[queue_name, 'queued'])

    action = request.POST.get('action', False)
    job_ids = request.POST.get('job_ids', False)
    if request.method != 'POST' or not action or not job_ids:
        return redirect(next_url)
    job_ids = request.POST.getlist('job_ids')
    if action == 'delete':
        for job_id in job_ids:
            job = JobExecution.fetch(job_id, connection=queue.connection)
            # Remove job id from queue and delete the actual job
            queue.remove_job_id(job.id)
            job.delete()
        messages.info(request, f'You have successfully deleted {len(job_ids)} jobs!')
    elif action == 'requeue':
        for job_id in job_ids:
            job = JobExecution.fetch(job_id, connection=queue.connection)
            job.requeue()
        messages.info(request, f'You have successfully re-queued {len(job_ids)}  jobs!')
    elif action == 'stop':
        cancelled_jobs = 0
        for job_id in job_ids:
            try:
                job = JobExecution.fetch(job_id, connection=queue.connection)
                job.stop_execution(queue.connection)
                job.cancel()
                cancelled_jobs += 1
            except Exception as e:
                logger.warning(f'Could not stop job: {e}')
                pass
        messages.info(request, f'You have successfully stopped {cancelled_jobs}  jobs!')
    return redirect(next_url)


SUPPORTED_JOB_ACTIONS = {'requeue', 'delete', 'enqueue', 'cancel'}


@never_cache
@staff_member_required
def job_action(request, job_id: str, action: str):
    queue, job = _find_job(job_id)
    if job is None:
        return HttpResponseBadRequest(f'Job {escape(job_id)} does not exist, maybe its TTL has passed')
    if action not in SUPPORTED_JOB_ACTIONS:
        return HttpResponseNotFound()

    if request.method != 'POST':
        context_data = {
            **admin.site.each_context(request),
            'job': job,
            'queue': queue,
            'action': action,
        }
        return render(request, 'admin/scheduler/single_job_action.html', context_data)

    try:
        if action == 'requeue':
            job.requeue()
            messages.info(request, f'You have successfully re-queued {job.id}')
            return redirect('job_details', job_id)
        elif action == 'delete':
            # Remove job id from queue and delete the actual job
            queue.remove_job_id(job.id)
            job.delete()
            messages.info(request, 'You have successfully deleted %s' % job.id)
            return redirect('queue_registry_jobs', queue.name, 'queued')
        elif action == 'enqueue':
            job.delete(remove_from_queue=False)
            queue._enqueue_job(job)
            messages.info(request, 'You have successfully enqueued %s' % job.id)
            return redirect('job_details', job_id)
        elif action == 'cancel':
            job.cancel()
            messages.info(request, 'You have successfully enqueued %s' % job.id)
            return redirect('job_details', job_id)
    except InvalidJobOperation as e:
        logger.warning(f'Could not perform action: {e}')
        messages.warning(request, f'Could not perform action: {e}')
    return redirect('job_details', job_id)
