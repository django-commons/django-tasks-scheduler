import os
import time
import traceback
from datetime import datetime
from enum import Enum
from logging import DEBUG, INFO
from threading import Thread
from typing import List, Set, Optional, Sequence, Dict

import django

from scheduler.helpers.queues import Queue
from scheduler.helpers.queues import get_queue
from scheduler.helpers.queues.getters import get_queue_connection
from scheduler.helpers.utils import current_timestamp
from scheduler.models import Task
from scheduler.redis_models import SchedulerLock, JobModel, ScheduledJobRegistry
from scheduler.settings import SCHEDULER_CONFIG, logger


class SchedulerStatus(str, Enum):
    STARTED = "started"
    WORKING = "working"
    STOPPED = "stopped"


def _reschedule_tasks() -> None:
    enabled_tasks = list(Task.objects.filter(enabled=True))
    for task in enabled_tasks:
        logger.debug(f"Rescheduling {str(task)}")
        task.save(schedule_job=True, clean=False)


class WorkerScheduler:
    def __init__(self, queues: Sequence[Queue], worker_name: str, interval: Optional[int] = None) -> None:
        self._queues = queues
        if len(queues) == 0:
            raise ValueError("At least one queue must be provided to WorkerScheduler")
        self._scheduled_job_registries: List[ScheduledJobRegistry] = []
        self.lock_acquisition_time: Optional[datetime] = None
        self._locks: Dict[str, SchedulerLock] = dict()
        self.connection = get_queue_connection(queues[0].name)
        self.interval = interval or SCHEDULER_CONFIG.SCHEDULER_INTERVAL
        self._stop_requested = False
        self.status = SchedulerStatus.STOPPED
        self._thread: Optional[Thread] = None
        self._pid: Optional[int] = None
        self.worker_name = worker_name

    @property
    def pid(self) -> Optional[int]:
        return self._pid

    def log(self, level: int, message: str, *args, **kwargs) -> None:
        logger.log(level, f"[Scheduler {self.worker_name}/{self._pid}]: {message}", *args, **kwargs)

    def _should_reacquire_locks(self) -> bool:
        """Returns True if lock_acquisition_time is longer than 10 minutes ago"""
        if not self.lock_acquisition_time:
            return True
        seconds_since = (datetime.now() - self.lock_acquisition_time).total_seconds()
        return seconds_since > SCHEDULER_CONFIG.SCHEDULER_FALLBACK_PERIOD_SECS

    def _acquire_locks(self) -> Set[str]:
        """Returns names of queue it successfully acquires lock on"""
        successful_locks = set()
        if self.pid is None:
            self._pid = os.getpid()
        queue_names = [queue.name for queue in self._queues]
        self.log(DEBUG, f"""Trying to acquire locks for {", ".join(queue_names)}""")
        for queue in self._queues:
            lock = SchedulerLock(queue.name)
            if lock.acquire(self.pid, connection=self.connection, expire=self.interval + 60):
                self._locks[queue.name] = lock
                successful_locks.add(queue.name)

        # Always reset _scheduled_job_registries when acquiring locks
        self.lock_acquisition_time = datetime.now()
        self._scheduled_job_registries = []
        for queue_name in self._locks:
            queue = get_queue(queue_name)
            self._scheduled_job_registries.append(queue.scheduled_job_registry)
        self.log(DEBUG, f"Locks acquired for {', '.join(self._locks.keys())}")
        return successful_locks

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
        """Updates the TTL on scheduler keys and the locks"""
        lock_keys = ", ".join(self._locks.keys())
        self.log(DEBUG, f"Scheduler updating lock for queue {lock_keys}")
        with self.connection.pipeline() as pipeline:
            for lock in self._locks.values():
                lock.expire(self.connection, expire=self.interval + 60)
            pipeline.execute()

    def stop(self) -> None:
        self.log(INFO, f"Stopping scheduler, releasing locks for {', '.join(self._locks.keys())}...")
        self.release_locks()
        self.status = SchedulerStatus.STOPPED

    def release_locks(self) -> None:
        """Release acquired locks"""
        with self.connection.pipeline() as pipeline:
            for lock in self._locks.values():
                lock.release(self.connection)
            pipeline.execute()

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
        _reschedule_tasks()

        for registry in self._scheduled_job_registries:
            timestamp = current_timestamp()
            job_names = registry.get_jobs_to_schedule(self.connection, timestamp)
            if len(job_names) == 0:
                continue
            queue = get_queue(registry.name)
            jobs = JobModel.get_many(job_names, connection=self.connection)
            with self.connection.pipeline() as pipeline:
                for job in jobs:
                    if job is not None:
                        queue.enqueue_job(job, pipeline=pipeline, at_front=job.at_front)
                pipeline.execute()
        self.status = SchedulerStatus.STARTED


def run_scheduler(scheduler: WorkerScheduler) -> None:
    try:
        scheduler.work()
    except Exception:  # noqa
        logger.error(f"Scheduler [PID {os.getpid()}] raised an exception.\n{traceback.format_exc()}")
        raise
    logger.info(f"Scheduler with PID {os.getpid()} has stopped")
