"""Keep one recurring chain per cron while preserving explicit manual executions.

All schedule transitions lock the Task row before reading Redis. Callbacks run before their
job leaves the active registry, and dequeue briefly leaves neither queued nor active membership.
The job record therefore matters as well as registry membership. A missing handoff gets its
timeout plus 60 seconds from the first observation before replacement. SQL and Redis are not
atomic: after an ambiguous enqueue the next tick adopts the persisted job rather than creating
another. Unreadable Redis never authorizes a new enqueue. See django-commons/django-tasks-scheduler#412.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from django.db import models
from django.utils import timezone

from scheduler.helpers.queues import Queue
from scheduler.redis_models import JobModel, JobStatus
from scheduler.redis_models.job import MISSING_REGISTRY_KEY_PREFIX
from scheduler.types.broker_types import BrokerErrorTypes

if TYPE_CHECKING:
    from scheduler.models import Task

logger = logging.getLogger(__name__)
_MANUAL = "scheduler_manual_run"
_SKIPPED = "scheduler_skipped_duplicate"
_RUNTIME_FIELDS = (
    "job_name",
    "scheduled_time",
    "successful_runs",
    "failed_runs",
    "last_successful_run",
    "last_failed_run",
    "created_at",
)
_SCHEDULE_FIELDS = ["job_name", "scheduled_time", "updated_at"]


def _copy_runtime(source: Task, target: Task) -> None:
    for field in _RUNTIME_FIELDS:
        setattr(target, field, getattr(source, field))


@dataclass
class Schedule:
    jobs: dict[str, JobModel]
    waiting: set[str]
    missing: set[str]


def read_schedule(task: Task, queue: Queue, exclude: str | None = None) -> Schedule:
    """Read live recurring jobs without mutating registries or the Task row."""
    names: set[str] = set()
    waiting: set[str] = set()
    scheduled: set[str] = set()
    for registry in (queue.scheduled_job_registry, queue.queued_job_registry, queue.active_job_registry):
        members = {
            name.decode() if isinstance(name, bytes) else name
            for name, _ in queue.connection.zscan_iter(registry.key, match=f"{task.queue}:{task.pk}:*", count=1000)
        }
        names.update(members)
        if registry is not queue.active_job_registry:
            waiting.update(members)
        if registry is queue.scheduled_job_registry:
            scheduled.update(members)
    registered = names.copy()
    if task.job_name:
        names.add(task.job_name)
    jobs: dict[str, JobModel] = {}
    for name in names:
        if name == exclude:
            waiting.discard(name)
            continue
        job = JobModel.get(name, connection=queue.connection)
        if job is None:
            continue
        if (
            job.meta.get(_MANUAL)
            or job.scheduled_task_id != task.pk
            or job.task_type != task.task_type
            or job.queue_name != task.queue
            or job.meta.get("scheduler_task_database", "default") != (task._state.db or "default")
        ):
            waiting.discard(name)
            continue
        if not same_generation(task, job):
            continue
        if job.status in {JobStatus.QUEUED, JobStatus.STARTED} or (
            job.status == JobStatus.SCHEDULED and name in scheduled
        ):
            jobs[name] = job
    return Schedule(jobs, waiting, set(jobs) - registered)


def _expire_lost_handoffs(queue: Queue, schedule: Schedule) -> None:
    now = timezone.now().timestamp()
    for name, job in list(schedule.jobs.items()):
        key = f"{MISSING_REGISTRY_KEY_PREFIX}{name}"
        if name not in schedule.missing:
            queue.connection.delete(key)
            continue
        # Start the grace period when membership is lost, not when a long-waiting job was enqueued.
        grace = max(job.timeout, 0) + 60
        deadline = queue.connection.get(key)
        if deadline is None:
            queue.connection.set(key, str(now + grace))
        elif now > float(deadline):
            queue.delete_job(name)
            queue.connection.delete(key)
            del schedule.jobs[name]


def _choose(task: Task, schedule: Schedule) -> str | None:
    if not task.enabled or not schedule.jobs:
        return None
    if task.job_name in schedule.jobs:
        return str(task.job_name)
    priority = {JobStatus.STARTED: 0, JobStatus.QUEUED: 1, JobStatus.SCHEDULED: 2}
    return min(schedule.jobs.values(), key=lambda job: (priority[job.status], job.created_at, job.name)).name


def _remove_waiting(queue: Queue, names: set[str]) -> None:
    for name in sorted(names):
        job = JobModel.get(name, connection=queue.connection)
        # A worker can dequeue while we hold the SQL lock; its execution guard handles that copy.
        if job is not None and (job.status == JobStatus.STARTED or job.meta.get(_MANUAL)):
            continue
        queue.delete_job(name)


def retire_schedule(task: Task) -> None:
    """Retire waiting jobs and handoff markers when a task changes identity or is disabled."""
    queue = task.rqueue
    schedule = read_schedule(task, queue)
    _remove_waiting(queue, schedule.waiting)
    names = schedule.missing | ({task.job_name} if task.job_name else set())
    for name in names:
        queue.connection.delete(f"{MISSING_REGISTRY_KEY_PREFIX}{name}")


def reconcile(task: Task, *, exclude: str | None = None, create: bool = True) -> bool:
    """Reconcile one cron while the caller holds its database row lock."""
    from scheduler.models.task import run_task

    queue = task.rqueue
    try:
        schedule = read_schedule(task, queue, exclude)
        _expire_lost_handoffs(queue, schedule)
        chosen = _choose(task, schedule)
        _remove_waiting(queue, schedule.waiting - ({chosen} if chosen else set()))
        if chosen is None and create and task.enabled:
            when = task._schedule_time()
            job = queue.create_and_enqueue_job(
                run_task,
                args=(task.task_type, task.pk),
                when=when,
                **task._enqueue_args(),
            )
            chosen = job.name
        if task.job_name and task.job_name != chosen:
            queue.connection.delete(f"{MISSING_REGISTRY_KEY_PREFIX}{task.job_name}")
        if exclude:
            queue.connection.delete(f"{MISSING_REGISTRY_KEY_PREFIX}{exclude}")
        task.job_name = chosen
        models.Model.save(task, using=task._state.db, update_fields=_SCHEDULE_FIELDS)
        return True
    except BrokerErrorTypes:
        # An ambiguous Redis write may already have created a job. The next tick must adopt it.
        logger.exception("Could not reconcile cron %s; leaving its schedule unchanged", task.name)
        return False


def same_generation(task: Task, job: JobModel) -> bool:
    created_at = task.created_at
    if timezone.is_naive(created_at):
        created_at = timezone.make_aware(created_at, timezone.get_default_timezone())
    return bool(job.created_at >= created_at)
