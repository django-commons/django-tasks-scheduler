import os
import time
import traceback
import uuid
from collections import defaultdict
from collections.abc import Collection, Sequence
from datetime import datetime
from enum import Enum
from logging import DEBUG, INFO, WARNING
from threading import Thread

import django

from scheduler.helpers.queues import Queue, get_queue
from scheduler.helpers.queues.getters import get_queue_connection
from scheduler.helpers.utils import current_timestamp, utcnow
from scheduler.models import Task
from scheduler.redis_models import JobModel, ScheduledJobRegistry, SchedulerLock
from scheduler.settings import SCHEDULER_CONFIG, logger


class SchedulerStatus(str, Enum):
    STARTED = "started"
    WORKING = "working"
    STOPPED = "stopped"


def _reschedule_tasks(queue_names: Collection[str]) -> None:
    """Give every enabled task on `queue_names` a pending job, if it has none.

    The tasks' job names are checked against the broker in one pipeline per queue, and a task is read in full only when
    it needs a new job - so a pass over tasks that are all scheduled costs a single query.
    """
    job_names_by_queue: dict[str, dict[int, str | None]] = defaultdict(dict)
    tasks = Task.objects.filter(enabled=True, queue__in=queue_names).values_list("id", "queue", "job_name")
    for task_id, queue_name, job_name in tasks:
        job_names_by_queue[queue_name][task_id] = job_name

    for queue_name, job_names in job_names_by_queue.items():
        pending = get_queue(queue_name).pending_job_names(name for name in job_names.values() if name)
        for task_id, job_name in job_names.items():
            if job_name in pending:
                continue
            # Read the task immediately before scheduling it: since the ids were read, a completion callback may have
            # given it a new job, or it may have been disabled or deleted.
            task = Task.objects.filter(id=task_id, enabled=True).first()
            if task is None:
                continue
            logger.debug(f"Rescheduling task {task.name}")
            try:
                task.reschedule_if_needed()
            except Exception:
                # One broken task must not stop the scheduler thread, which would leave every other task unscheduled.
                logger.exception(f"Failed to reschedule task {task.name}")


class WorkerScheduler:
    def __init__(self, queues: Sequence[Queue], worker_name: str, interval: int | None = None) -> None:
        self._queues = queues
        if len(queues) == 0:
            raise ValueError("At least one queue must be provided to WorkerScheduler")
        self._scheduled_job_registries: list[ScheduledJobRegistry] = []
        self.lock_acquisition_time: datetime | None = None
        self._locks: dict[str, SchedulerLock] = {}
        self.connection = get_queue_connection(queues[0].name)
        self.interval = interval or SCHEDULER_CONFIG.SCHEDULER_INTERVAL
        self._stop_requested = False
        self.status = SchedulerStatus.STOPPED
        self._thread: Thread | None = None
        self._pid: int | None = None
        self._lock_token: str | None = None
        self.worker_name = worker_name

    @property
    def pid(self) -> int | None:
        return self._pid

    def log(self, level: int, message: str, *args, **kwargs) -> None:
        logger.log(level, f"[Scheduler {self.worker_name}/{self._pid}]: {message}", *args, **kwargs)

    def _should_reacquire_locks(self) -> bool:
        """Returns True if lock_acquisition_time is longer than 10 minutes ago"""
        if not self.lock_acquisition_time:
            return True
        seconds_since = (utcnow() - self.lock_acquisition_time).total_seconds()
        return seconds_since > SCHEDULER_CONFIG.SCHEDULER_FALLBACK_PERIOD_SECS

    def _acquire_locks(self) -> set[str]:
        """Returns names of queue it successfully acquires lock on"""
        successful_locks = set()
        if self.pid is None:
            self._pid = os.getpid()
            # The pid alone is not unique across hosts and containers, so the lock token adds a random part. The pid
            # prefix is what `Queue.scheduler_pid` reports.
            self._lock_token = f"{self._pid}:{uuid.uuid4().hex}"
        queue_names = [queue.name for queue in self._queues]
        self.log(DEBUG, f"""Trying to acquire locks for {", ".join(queue_names)}""")
        for queue in self._queues:
            lock = SchedulerLock(queue.name)
            if lock.acquire(self._lock_token, connection=self.connection, expire=self.interval + 60):
                self._locks[queue.name] = lock
                successful_locks.add(queue.name)

        self.lock_acquisition_time = utcnow()
        self._refresh_scheduled_job_registries()
        self.log(DEBUG, f"Locks acquired for {', '.join(self._locks.keys())}")
        return successful_locks

    def _refresh_scheduled_job_registries(self) -> None:
        self._scheduled_job_registries = [get_queue(queue_name).scheduled_job_registry for queue_name in self._locks]

    def start(self) -> None:
        locks = self._acquire_locks()
        if len(locks) == 0:
            return
        self.status = SchedulerStatus.STARTED
        self._thread = Thread(target=run_scheduler, args=(self,), name="scheduler-thread")
        self._thread.start()

    def request_stop_and_wait(self) -> None:
        """Toggle self._stop_requested that's checked on every loop"""
        self.log(DEBUG, "Stop Scheduler requested")
        self._stop_requested = True
        if self._thread is not None:
            self._thread.join()

    def heartbeat(self) -> None:
        """Extends the locks this scheduler still holds, and stops scheduling the queues whose lock it lost."""
        lock_keys = ", ".join(self._locks.keys())
        self.log(DEBUG, f"Scheduler updating lock for queue {lock_keys}")
        lost = [
            queue_name
            for queue_name, lock in self._locks.items()
            if not lock.expire(self.connection, expire=self.interval + 60)
        ]
        for queue_name in lost:
            # The lock expired and another scheduler took it over: carrying on would schedule this queue twice.
            self.log(WARNING, f"Lost the scheduler lock for queue {queue_name}, no longer scheduling it")
            del self._locks[queue_name]
        if lost:
            self._refresh_scheduled_job_registries()

    def stop(self) -> None:
        self.log(INFO, f"Stopping scheduler, releasing locks for {', '.join(self._locks.keys())}...")
        self.release_locks()
        self.status = SchedulerStatus.STOPPED

    def release_locks(self) -> None:
        """Release acquired locks"""
        for lock in self._locks.values():
            lock.release(self.connection)

    def work(self) -> None:
        queue_names = [queue.name for queue in self._queues]
        self.log(INFO, f"""Scheduler for {", ".join(queue_names)} started""")
        django.setup()

        while True:
            if self._stop_requested:
                self.stop()
                break

            if self._should_reacquire_locks():
                self._acquire_locks()

            self.enqueue_scheduled_jobs()
            self.heartbeat()
            time.sleep(self.interval)

    def enqueue_scheduled_jobs(self) -> None:
        """Enqueue jobs whose timestamp is in the past"""
        self.status = SchedulerStatus.WORKING
        # Only the queues this scheduler holds the lock for: another scheduler owns the rest.
        _reschedule_tasks(queue_names=list(self._locks))

        for registry in self._scheduled_job_registries:
            timestamp = current_timestamp()
            job_names = registry.get_jobs_to_schedule(self.connection, timestamp)
            if len(job_names) == 0:
                continue
            queue = get_queue(registry.name)
            jobs = JobModel.get_many(job_names, connection=self.connection)
            with self.connection.pipeline() as pipeline:
                for job_name, job in zip(job_names, jobs):
                    if job is not None:
                        queue.enqueue_job(job, pipeline=pipeline, at_front=job.at_front)
                    else:
                        registry.delete(connection=pipeline, job_name=job_name)
                pipeline.execute()
        self.status = SchedulerStatus.STARTED


def run_scheduler(scheduler: WorkerScheduler) -> None:
    try:
        scheduler.work()
    except Exception:
        logger.error(f"Scheduler [PID {os.getpid()}] raised an exception.\n{traceback.format_exc()}")
        raise
    logger.info(f"Scheduler with PID {os.getpid()} has stopped")
