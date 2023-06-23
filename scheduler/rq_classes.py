from typing import List, Any, Optional, Union

import django
from django.apps import apps
from redis import Redis
from redis.client import Pipeline
from rq import Worker
from rq.command import send_stop_job_command
from rq.decorators import job
from rq.exceptions import InvalidJobOperation
from rq.job import Job, JobStatus
from rq.job import get_current_job  # noqa
from rq.queue import Queue, logger
from rq.registry import (
    DeferredJobRegistry, FailedJobRegistry, FinishedJobRegistry,
    ScheduledJobRegistry, StartedJobRegistry, CanceledJobRegistry, BaseRegistry,
)
from rq.scheduler import RQScheduler
from rq.worker import WorkerStatus

from scheduler import settings

MODEL_NAMES = ['ScheduledJob', 'RepeatableJob', 'CronJob']

rq_job_decorator = job
ExecutionStatus = JobStatus
InvalidJobOperation = InvalidJobOperation


def as_text(v: Union[bytes, str]) -> Optional[str]:
    """Converts a bytes value to a string using `utf-8`.

    :param v: The value (bytes or string)
    :raises: ValueError: If the value is not bytes or string
    :returns: Either the decoded string or None
    """
    if v is None:
        return None
    elif isinstance(v, bytes):
        return v.decode('utf-8')
    elif isinstance(v, str):
        return v
    else:
        raise ValueError('Unknown type %r' % type(v))


def compact(lst: List[Any]) -> List[Any]:
    """Remove `None` values from an iterable object.
    :param lst: A list (or list-like) object
    :returns: The list without None values
    """
    return [item for item in lst if item is not None]


class JobExecution(Job):
    def __eq__(self, other):
        return isinstance(other, Job) and self.id == other.id

    @property
    def is_scheduled_job(self):
        return self.meta.get('scheduled_job_id', None) is not None

    def is_execution_of(self, scheduled_job):
        return (self.meta.get('job_type', None) == scheduled_job.JOB_TYPE
                and self.meta.get('scheduled_job_id', None) == scheduled_job.id)

    def stop_execution(self, connection: Redis):
        send_stop_job_command(connection, self.id)


class DjangoWorker(Worker):
    def __init__(self, *args, **kwargs):
        self.fork_job_execution = kwargs.pop('fork_job_execution', True)
        kwargs['job_class'] = JobExecution
        kwargs['queue_class'] = DjangoQueue
        super(DjangoWorker, self).__init__(*args, **kwargs)

    def __eq__(self, other):
        return (isinstance(other, Worker)
                and self.key == other.key
                and self.name == other.name)

    def __hash__(self):
        return hash((self.name, self.key, ','.join(self.queue_names())))

    def __str__(self):
        return f"{self.name}/{','.join(self.queue_names())}"

    def _start_scheduler(
            self,
            burst: bool = False,
            logging_level: str = "INFO",
            date_format: str = '%H:%M:%S',
            log_format: str = '%(asctime)s %(message)s',
    ) -> None:
        """Starts the scheduler process.
        This is specifically designed to be run by the worker when running the `work()` method.
        Instantiates the DjangoScheduler and tries to acquire a lock.
        If the lock is acquired, start scheduler.
        If worker is on burst mode just enqueues scheduled jobs and quits,
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
                self._set_property('scheduler_pid', proc.pid)

    def execute_job(self, job: 'Job', queue: 'Queue'):
        if self.fork_job_execution:
            super(DjangoWorker, self).execute_job(job, queue)
        else:
            self.set_state(WorkerStatus.BUSY)
            self.perform_job(job, queue)
            self.set_state(WorkerStatus.IDLE)

    def work(self, **kwargs) -> bool:
        kwargs.setdefault('with_scheduler', True)
        return super(DjangoWorker, self).work(**kwargs)

    def _set_property(self, prop_name: str, val, pipeline: Optional[Pipeline] = None):
        connection = pipeline if pipeline is not None else self.connection
        if val is None:
            connection.hdel(self.key, prop_name)
        else:
            connection.hset(self.key, prop_name, val)

    def _get_property(self, prop_name: str, pipeline: Optional[Pipeline] = None):
        connection = pipeline if pipeline is not None else self.connection
        return as_text(connection.hget(self.key, prop_name))

    def scheduler_pid(self) -> int:
        pid = self.connection.get(RQScheduler.get_locking_key(self.queues[0].name))
        return int(pid.decode()) if pid is not None else None


class DjangoQueue(Queue):
    REGISTRIES = dict(
        finished='finished_job_registry',
        failed='failed_job_registry',
        scheduled='scheduled_job_registry',
        started='started_job_registry',
        deferred='deferred_job_registry',
        canceled='canceled_job_registry',
    )
    """
    A subclass of RQ's QUEUE that allows jobs to be stored temporarily to be
    enqueued later at the end of Django's request/response cycle.
    """

    def __init__(self, *args, **kwargs):
        kwargs['job_class'] = JobExecution
        super(DjangoQueue, self).__init__(*args, **kwargs)

    def get_registry(self, name: str) -> Union[None, BaseRegistry, 'DjangoQueue']:
        name = name.lower()
        if name == 'queued':
            return self
        elif name in DjangoQueue.REGISTRIES:
            return getattr(self, DjangoQueue.REGISTRIES[name])
        return None

    @property
    def finished_job_registry(self):
        return FinishedJobRegistry(self.name, self.connection)

    @property
    def started_job_registry(self):
        return StartedJobRegistry(self.name, self.connection, job_class=JobExecution, )

    @property
    def deferred_job_registry(self):
        return DeferredJobRegistry(self.name, self.connection, job_class=JobExecution, )

    @property
    def failed_job_registry(self):
        return FailedJobRegistry(self.name, self.connection, job_class=JobExecution, )

    @property
    def scheduled_job_registry(self):
        return ScheduledJobRegistry(self.name, self.connection, job_class=JobExecution, )

    @property
    def canceled_job_registry(self):
        return CanceledJobRegistry(self.name, self.connection, job_class=JobExecution, )

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

    def get_all_jobs(self):
        job_ids = self.get_all_job_ids()
        return compact([self.fetch_job(job_id) for job_id in job_ids])

    def clean_registries(self):
        self.started_job_registry.cleanup()
        self.failed_job_registry.cleanup()
        self.finished_job_registry.cleanup()

    def remove_job_id(self, job_id: str):
        self.connection.lrem(self.key, 0, job_id)

    def last_job_id(self):
        return self.connection.lindex(self.key, 0)


class DjangoScheduler(RQScheduler):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('interval', settings.SCHEDULER_CONFIG['SCHEDULER_INTERVAL'])
        super(DjangoScheduler, self).__init__(*args, **kwargs)

    @staticmethod
    def reschedule_all_jobs():
        logger.debug("Rescheduling all jobs")
        for model_name in MODEL_NAMES:
            model = apps.get_model(app_label='scheduler', model_name=model_name)
            enabled_jobs = model.objects.filter(enabled=True)
            unscheduled_jobs = filter(lambda j: j.ready_for_schedule(), enabled_jobs)
            for item in unscheduled_jobs:
                logger.debug(f"Rescheduling {str(item)}")
                item.save()

    def work(self):
        django.setup()
        super(DjangoScheduler, self).work()

    def enqueue_scheduled_jobs(self):
        self.reschedule_all_jobs()
        super(DjangoScheduler, self).enqueue_scheduled_jobs()
