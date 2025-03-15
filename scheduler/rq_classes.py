from typing import List, Optional, Union

import django
from django.apps import apps
from rq import Worker
from rq.command import send_stop_job_command
from rq.decorators import job
from rq.exceptions import InvalidJobOperation
from rq.job import Job, JobStatus
from rq.job import get_current_job  # noqa
from rq.queue import Queue, logger
from rq.registry import (
    DeferredJobRegistry,
    FailedJobRegistry,
    FinishedJobRegistry,
    ScheduledJobRegistry,
    StartedJobRegistry,
    CanceledJobRegistry,
    BaseRegistry,
)
from rq.scheduler import RQScheduler
from rq.worker import WorkerStatus

from scheduler import settings
from scheduler.broker_types import PipelineType, ConnectionType

MODEL_NAMES = ["Task"]
TASK_TYPES = ["OnceTaskType", "RepeatableTaskType", "CronTaskType"]

rq_job_decorator = job
ExecutionStatus = JobStatus
InvalidJobOperation = InvalidJobOperation


def register_sentry(sentry_dsn, **opts):
    from rq.contrib.sentry import register_sentry as rq_register_sentry

    rq_register_sentry(sentry_dsn, **opts)


def as_str(v: Union[bytes, str]) -> Optional[str]:
    """Converts a `bytes` value to a string using `utf-8`.

    :param v: The value (None/bytes/str)
    :raises: ValueError: If the value is not `bytes` or `str`
    :returns: Either the decoded string or None
    """
    if v is None:
        return None
    if isinstance(v, bytes):
        return v.decode("utf-8")
    if isinstance(v, str):
        return v
    raise ValueError("Unknown type %r" % type(v))


class JobExecution(Job):
    def __eq__(self, other) -> bool:
        return isinstance(other, Job) and self.id == other.id

    @property
    def is_scheduled_task(self) -> bool:
        return self.meta.get("scheduled_task_id", None) is not None

    def is_execution_of(self, task: "Task") -> bool:  # noqa: F821
        return (
            self.meta.get("task_type", None) == task.task_type and self.meta.get("scheduled_task_id", None) == task.id
        )

    def stop_execution(self, connection: ConnectionType):
        send_stop_job_command(connection, self.id)


class DjangoWorker(Worker):
    def __init__(self, *args, **kwargs):
        self.fork_job_execution = kwargs.pop("fork_job_execution", True)
        job_class = kwargs.get("job_class") or JobExecution
        if not isinstance(job_class, type) or not issubclass(job_class, JobExecution):
            raise ValueError("job_class must be a subclass of JobExecution")

        # Update kwargs with the potentially modified job_class
        kwargs["job_class"] = job_class
        kwargs["queue_class"] = DjangoQueue
        super(DjangoWorker, self).__init__(*args, **kwargs)

    def __eq__(self, other):
        return isinstance(other, Worker) and self.key == other.key and self.name == other.name

    def __hash__(self):
        return hash((self.name, self.key, ",".join(self.queue_names())))

    def __str__(self):
        return f"{self.name}/{','.join(self.queue_names())}"

    def _start_scheduler(
        self,
        burst: bool = False,
        logging_level: str = "INFO",
        date_format: str = "%H:%M:%S",
        log_format: str = "%(asctime)s %(message)s",
    ) -> None:
        """Starts the scheduler process.
        This is specifically designed to be run by the worker when running the `work()` method.
        Instantiates the DjangoScheduler and tries to acquire a lock.
        If the lock is acquired, start scheduler.
        If the worker is on burst mode, just enqueues scheduled jobs and quits,
        otherwise, starts the scheduler in a separate process.

        :param burst (bool, optional): Whether to work on burst mode. Defaults to False.
        :param logging_level (str, optional): Logging level to use. Defaults to "INFO".
        :param date_format (str, optional): Date Format. Defaults to DEFAULT_LOGGING_DATE_FORMAT.
        :param log_format (str, optional): Log Format. Defaults to DEFAULT_LOGGING_FORMAT.
        """
        self.scheduler = DjangoScheduler(
            self.queues,
            connection=self.connection,
            logging_level=logging_level,
            date_format=date_format,
            log_format=log_format,
            serializer=self.serializer,
        )
        self.scheduler.acquire_locks()
        if self.scheduler.acquired_locks:
            if burst:
                self.scheduler.enqueue_scheduled_jobs()
                self.scheduler.release_locks()
            else:
                proc = self.scheduler.start()
                self._set_property("scheduler_pid", proc.pid)

    def execute_job(self, job: "Job", queue: "Queue") -> None:
        if self.fork_job_execution:
            super(DjangoWorker, self).execute_job(job, queue)
        else:
            self.set_state(WorkerStatus.BUSY)
            self.perform_job(job, queue)
            self.set_state(WorkerStatus.IDLE)

    def work(self, **kwargs) -> bool:
        kwargs.setdefault("with_scheduler", True)
        return super(DjangoWorker, self).work(**kwargs)

    def _set_property(self, prop_name: str, val, pipeline: Optional[PipelineType] = None) -> None:
        connection = pipeline if pipeline is not None else self.connection
        if val is None:
            connection.hdel(self.key, prop_name)
        else:
            connection.hset(self.key, prop_name, val)

    def _get_property(self, prop_name: str, pipeline: Optional[PipelineType] = None) -> Optional[str]:
        connection = pipeline if pipeline is not None else self.connection
        res = connection.hget(self.key, prop_name)
        return as_str(res)

    def scheduler_pid(self) -> Optional[int]:
        if len(self.queues) == 0:
            logger.warning("No queues to get scheduler pid from")
            return None
        pid = self.connection.get(DjangoScheduler.get_locking_key(self.queues[0].name))
        return int(pid.decode()) if pid is not None else None


class DjangoQueue(Queue):
    """A subclass of RQ's QUEUE that allows jobs to be stored temporarily to be enqueued later at the end of Django's
    request/response cycle."""

    REGISTRIES = dict(
        finished="finished_job_registry",
        failed="failed_job_registry",
        scheduled="scheduled_job_registry",
        started="started_job_registry",
        deferred="deferred_job_registry",
        canceled="canceled_job_registry",
    )

    def __init__(self, *args, **kwargs) -> None:
        kwargs["job_class"] = JobExecution
        super(DjangoQueue, self).__init__(*args, **kwargs)

    def get_registry(self, name: str) -> Union[None, BaseRegistry, "DjangoQueue"]:
        name = name.lower()
        if name == "queued":
            return self
        elif name in DjangoQueue.REGISTRIES:
            return getattr(self, DjangoQueue.REGISTRIES[name])
        return None

    @property
    def finished_job_registry(self) -> FinishedJobRegistry:
        return FinishedJobRegistry(self.name, self.connection)

    @property
    def started_job_registry(self) -> StartedJobRegistry:
        return StartedJobRegistry(
            self.name,
            self.connection,
            job_class=JobExecution,
        )

    @property
    def deferred_job_registry(self) -> DeferredJobRegistry:
        return DeferredJobRegistry(
            self.name,
            self.connection,
            job_class=JobExecution,
        )

    @property
    def failed_job_registry(self) -> FailedJobRegistry:
        return FailedJobRegistry(
            self.name,
            self.connection,
            job_class=JobExecution,
        )

    @property
    def scheduled_job_registry(self) -> ScheduledJobRegistry:
        return ScheduledJobRegistry(
            self.name,
            self.connection,
            job_class=JobExecution,
        )

    @property
    def canceled_job_registry(self) -> CanceledJobRegistry:
        return CanceledJobRegistry(
            self.name,
            self.connection,
            job_class=JobExecution,
        )

    def get_all_job_ids(self) -> List[str]:
        res = list()
        res.extend(self.get_job_ids())
        res.extend(self.finished_job_registry.get_job_ids())
        res.extend(self.started_job_registry.get_job_ids())
        res.extend(self.deferred_job_registry.get_job_ids())
        res.extend(self.failed_job_registry.get_job_ids())
        res.extend(self.scheduled_job_registry.get_job_ids())
        res.extend(self.canceled_job_registry.get_job_ids())
        return res

    def get_all_jobs(self) -> List[JobExecution]:
        job_ids = self.get_all_job_ids()
        return list(filter(lambda j: j is not None, [self.fetch_job(job_id) for job_id in job_ids]))

    def clean_registries(self) -> None:
        self.started_job_registry.cleanup()
        self.failed_job_registry.cleanup()
        self.finished_job_registry.cleanup()

    def remove_job_id(self, job_id: str) -> None:
        self.connection.lrem(self.key, 0, job_id)

    def last_job_id(self) -> Optional[str]:
        return self.connection.lindex(self.key, 0)


class DjangoScheduler(RQScheduler):
    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("interval", settings.SCHEDULER_CONFIG.SCHEDULER_INTERVAL)
        super(DjangoScheduler, self).__init__(*args, **kwargs)

    @staticmethod
    def reschedule_all_jobs():
        for model_name in MODEL_NAMES:
            model = apps.get_model(app_label="scheduler", model_name=model_name)
            enabled_jobs = model.objects.filter(enabled=True)
            for item in enabled_jobs:
                logger.debug(f"Rescheduling {str(item)}")
                item.save()

    def work(self) -> None:
        django.setup()
        super(DjangoScheduler, self).work()

    def enqueue_scheduled_jobs(self) -> None:
        self.reschedule_all_jobs()
        super(DjangoScheduler, self).enqueue_scheduled_jobs()
