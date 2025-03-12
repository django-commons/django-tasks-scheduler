import os
import signal
import time
import traceback
from datetime import datetime
from enum import Enum
from multiprocessing import Process
from typing import List, Set, Optional, Sequence, Dict

import django
from django.apps import apps

from scheduler.broker_types import ConnectionType, MODEL_NAMES
from scheduler.helpers.queues import get_queue
from scheduler.redis_models import SchedulerLock, JobModel
from scheduler.redis_models.registry import ScheduledJobRegistry
from scheduler.helpers.queues import Queue
from scheduler.settings import SCHEDULER_CONFIG, logger
from scheduler.helpers.utils import current_timestamp


class SchedulerStatus(str, Enum):
    STARTED = "started"
    WORKING = "working"
    STOPPED = "stopped"


def _reschedule_all_jobs():
    for model_name in MODEL_NAMES:
        model = apps.get_model(app_label="scheduler", model_name=model_name)
        enabled_jobs = model.objects.filter(enabled=True)
        for item in enabled_jobs:
            logger.debug(f"Rescheduling {str(item)}")
            item.save()


class WorkerScheduler:
    def __init__(
            self,
            queues: Sequence[Queue],
            connection: ConnectionType,
            interval: Optional[int] = None,
    ) -> None:
        interval = interval or SCHEDULER_CONFIG.SCHEDULER_INTERVAL
        self._queue_names = {queue.name for queue in queues}
        self._scheduled_job_registries: List[ScheduledJobRegistry] = []
        self.lock_acquisition_time = None
        self._pool_class = connection.connection_pool.connection_class
        self._pool_kwargs = connection.connection_pool.connection_kwargs.copy()
        self._locks: Dict[str, SchedulerLock] = dict()
        self.connection = connection
        self.interval = interval
        self._stop_requested = False
        self._status = SchedulerStatus.STOPPED
        self._process = None
        self._pid: Optional[int] = None

    @property
    def pid(self) -> Optional[int]:
        return self._pid

    def _should_reacquire_locks(self) -> bool:
        """Returns True if lock_acquisition_time is longer than 10 minutes ago"""
        if not self.lock_acquisition_time:
            return True
        seconds_since = (datetime.now() - self.lock_acquisition_time).total_seconds()
        return seconds_since > SCHEDULER_CONFIG.SCHEDULER_FALLBACK_PERIOD_SECS

    def _acquire_locks(self) -> Set[str]:
        """Returns names of queue it successfully acquires lock on"""
        successful_locks = set()
        pid = os.getpid()
        logger.debug("Trying to acquire locks for %s", ", ".join(self._queue_names))
        for queue_name in self._queue_names:
            lock = SchedulerLock(queue_name)
            if lock.acquire(pid, connection=self.connection, expire=self.interval + 60):
                self._locks[queue_name] = lock

        # Always reset _scheduled_job_registries when acquiring locks
        self.lock_acquisition_time = datetime.now()
        self._scheduled_job_registries = []
        for queue_name in self._locks:
            queue = get_queue(queue_name)
            self._scheduled_job_registries.append(queue.scheduled_job_registry)

        return successful_locks

    def start(self, burst=False) -> None:
        locks = self._acquire_locks()
        if len(locks) == 0:
            return
        if burst:
            self.enqueue_scheduled_jobs()
            self.release_locks()
            return
        self._status = SchedulerStatus.STARTED
        self._process = Process(target=run_scheduler, args=(self,), name="Scheduler")
        self._process.start()
        self._pid = self._process.pid

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM"""
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def request_stop(self, signum=None, frame=None):
        """Toggle self._stop_requested that's checked on every loop"""
        self._stop_requested = True

    def heartbeat(self):
        """Updates the TTL on scheduler keys and the locks"""
        lock_keys= ", ".join(self._locks.keys())
        logger.debug(f"Scheduler sending heartbeat to {lock_keys}")
        with self.connection.pipeline() as pipeline:
            for lock in self._locks.values():
                lock.expire(self.connection, expire=self.interval + 60)
            pipeline.execute()

    def stop(self):
        logger.info(f"Stopping scheduler, releasing locks for {', '.join(self._locks.keys())}...")
        self.release_locks()
        self._status = SchedulerStatus.STOPPED

    def release_locks(self):
        """Release acquired locks"""
        with self.connection.pipeline() as pipeline:
            for lock in self._locks.values():
                lock.release(self.connection)
            pipeline.execute()

    def work(self) -> None:
        logger.info(f"""Scheduler for {", ".join(self._queue_names)} started with PID {os.getpid()}""")
        django.setup()
        self._install_signal_handlers()

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
        self._status = SchedulerStatus.WORKING
        _reschedule_all_jobs()

        for registry in self._scheduled_job_registries:
            timestamp = current_timestamp()
            job_ids = registry.get_jobs_to_schedule(timestamp)

            if not job_ids:
                continue

            queue = get_queue(registry.name)

            with self.connection.pipeline() as pipeline:
                jobs = JobModel.get_many(job_ids, connection=self.connection)
                for job in jobs:
                    if job is not None:
                        queue._enqueue_job(job, connection=pipeline, at_front=bool(job.at_front))
                        registry.delete(pipeline, job.name)
                pipeline.execute()
        self._status = SchedulerStatus.STARTED


def run_scheduler(scheduler):
    try:
        scheduler.work()
    except:  # noqa
        logger.error(f"Scheduler [PID {os.getpid()}] raised an exception.\n{traceback.format_exc()}")
        raise
    logger.info(f"Scheduler with PID %{os.getpid()} has stopped")
