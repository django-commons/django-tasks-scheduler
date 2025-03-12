import asyncio
import sys
import traceback
from datetime import datetime, timedelta
from functools import total_ordering
from typing import Dict, List, Optional, Tuple, Union, Self, Any

from redis import WatchError

from scheduler.broker_types import ConnectionType, FunctionReferenceType
from scheduler.helpers.utils import utcnow, current_timestamp
from scheduler.redis_models import as_str, JobStatus, Callback, SchedulerLock, Result, ResultType, JobModel
from scheduler.redis_models.registry import (
    JobNamesRegistry, FinishedJobRegistry, StartedJobRegistry,
    FailedJobRegistry,
    CanceledJobRegistry, ScheduledJobRegistry, QueuedJobRegistry,
    NoSuchJobError, )
from scheduler.settings import logger, SCHEDULER_CONFIG


class InvalidJobOperation(Exception):
    pass


def perform_job(job_model: JobModel, connection: ConnectionType) -> Any:  # noqa
    """The main execution method. Invokes the job function with the job arguments.

    :returns: The job's return value
    """
    job_model.persist(connection=connection)
    _job_stack.append(job_model)
    try:
        result = job_model.func(*job_model.args, **job_model.kwargs)
        if asyncio.iscoroutine(result):
            loop = asyncio.new_event_loop()
            coro_result = loop.run_until_complete(result)
            result = coro_result
    finally:
        assert job_model is _job_stack.pop()
    return result


_job_stack = []


@total_ordering
class Queue:
    REGISTRIES = dict(
        finished="finished_job_registry",
        failed="failed_job_registry",
        scheduled="scheduled_job_registry",
        started="started_job_registry",
        canceled="canceled_job_registry",
        queued="queued_job_registry",
    )

    def __init__(self, connection: Optional[ConnectionType], name: str, is_async: bool = True) -> None:
        """Initializes a Queue object.

        :param name: The queue name
        :param connection: Broker connection
        :param is_async: Whether jobs should run "async" (using the worker).
        """
        self.connection = connection
        self.name = name
        self._is_async = is_async
        self.queued_job_registry = QueuedJobRegistry(connection=self.connection, name=self.name)
        self.started_job_registry = StartedJobRegistry(connection=self.connection, name=self.name)
        self.failed_job_registry = FailedJobRegistry(connection=self.connection, name=self.name)
        self.finished_job_registry = FinishedJobRegistry(connection=self.connection, name=self.name)
        self.scheduled_job_registry = ScheduledJobRegistry(connection=self.connection, name=self.name)
        self.canceled_job_registry = CanceledJobRegistry(connection=self.connection, name=self.name)

    def __len__(self):
        return self.count

    @property
    def scheduler_pid(self) -> int:
        lock = SchedulerLock(self.name)
        pid = lock.value(self.connection)
        return int(pid.decode()) if pid is not None else None

    def clean_registries(self, timestamp: Optional[float] = None) -> None:
        """Remove abandoned jobs from registry and add them to FailedJobRegistry.

           Removes jobs with an expiry time earlier than current_timestamp, specified as seconds since the Unix epoch.
           Removed jobs are added to the global failed job queue.
        """
        before_score = timestamp or current_timestamp()
        started_jobs: List[Tuple[str, float]] = self.started_job_registry.get_job_names_before(
            self.connection, before_score)

        with self.connection.pipeline() as pipeline:
            for job_name, job_score in started_jobs:
                job = JobModel.get(job_name, connection=self.connection)
                if job is None or job.failure_callback is None or job_score + job.timeout > before_score:
                    continue

                logger.debug(f"Running failure callbacks for {job.name}")
                try:
                    job.failure_callback(job, self.connection, traceback.extract_stack())
                except Exception:  # noqa
                    logger.exception(f"Job {self.name}: error while executing failure callback")
                    raise

                retry = job.retries_left and job.retries_left > 0

                if retry:
                    self.retry_job(job, pipeline)

                else:
                    logger.warning(
                        f"{self.__class__.__name__} cleanup: Moving job to {self.failed_job_registry.key} "
                        f"(due to AbandonedJobError)"
                    )
                    job.set_status(JobStatus.FAILED, connection=pipeline)
                    exc_string = f"Moved to {self.failed_job_registry.key}, due to AbandonedJobError, at {datetime.now()}"
                    job.save(connection=pipeline)
                    job.expire(ttl=-1, connection=pipeline)
                    score = current_timestamp() + SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL
                    Result.create(
                        connection=pipeline,
                        job_name=job.name,
                        _type=ResultType.FAILED,
                        ttl=SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL,
                        exc_string=exc_string,
                    )
                    self.failed_job_registry.add(pipeline, job.name, score)
                    job.save(connection=pipeline)
                    job.expire(connection=pipeline, ttl=SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL)

            for registry in self.REGISTRIES.values():
                getattr(self, registry).cleanup(connection=self.connection, timestamp=before_score)
            pipeline.execute()

    def first_queued_job_name(self) -> Optional[str]:
        return self.queued_job_registry.get_first()

    def empty(self):
        """Removes all queued jobs from the queue."""
        queued_jobs_count = self.queued_job_registry.count(connection=self.connection)
        with self.connection.pipeline() as pipe:
            for offset in range(0, queued_jobs_count, 1000):
                job_names = self.queued_job_registry.all(offset, 1000)
                for job_name in job_names:
                    self.queued_job_registry.delete(connection=pipe, job_name=job_name)
                JobModel.delete_many(job_names, connection=pipe)
            pipe.execute()

    @property
    def count(self) -> int:
        """Returns a count of all messages in the queue."""
        res = 0
        for registry in self.REGISTRIES.values():
            res += getattr(self, registry).count(connection=self.connection)
        return res

    def get_registry(self, name: str) -> Union[None, JobNamesRegistry]:
        name = name.lower()
        if name in Queue.REGISTRIES:
            return getattr(self, Queue.REGISTRIES[name])
        return None

    def get_all_job_ids(self) -> List[str]:
        res = list()
        res.extend(self.queued_job_registry.all())
        res.extend(self.finished_job_registry.all())
        res.extend(self.started_job_registry.all())
        res.extend(self.failed_job_registry.all())
        res.extend(self.scheduled_job_registry.all())
        res.extend(self.canceled_job_registry.all())
        return res

    def get_all_jobs(self) -> List[JobModel]:
        job_ids = self.get_all_job_ids()
        return JobModel.get_many(job_ids, connection=self.connection)

    def enqueue_call(
            self,
            func: FunctionReferenceType,
            args: Union[Tuple, List, None] = None,
            kwargs: Optional[Dict] = None,
            timeout: Optional[int] = None,
            result_ttl: Optional[int] = None,
            ttl: Optional[int] = None,
            description: Optional[str] = None,
            name: Optional[str] = None,
            at_front: bool = False,
            meta: Optional[Dict] = None,
            retries_left: Optional[int] = None,
            retry_intervals: Union[int, List[int], None] = None,
            on_success: Optional[Callback] = None,
            on_failure: Optional[Callback] = None,
            on_stopped: Optional[Callback] = None,
            task_type: Optional[str] = None,
            scheduled_task_id: Optional[int] = None,
            pipeline: Optional[ConnectionType] = None,
    ) -> JobModel:
        """Creates a job to represent the delayed function call and enqueues it.

        :param func: The reference to the function
        :param args: The `*args` to pass to the function
        :param kwargs: The `**kwargs` to pass to the function
        :param timeout: Function timeout
        :param result_ttl: Result time to live
        :param ttl: Time to live
        :param description: The job description
        :param name: The job name
        :param at_front: Whether to enqueue the job at the front
        :param meta: Metadata to attach to the job
        :param retries_left: Number of retries left
        :param retry_intervals: List of retry intervals
        :param on_success: Callback for on success
        :param on_failure: Callback for on failure
        :param on_stopped: Callback for on stopped
        :param task_type: The task type
        :param scheduled_task_id: The scheduled task id
        :param pipeline: The Redis Pipeline
        :returns: The enqueued Job
        """

        job_model = JobModel.create(
            connection=self.connection,
            func=func,
            args=args,
            kwargs=kwargs,
            result_ttl=result_ttl,
            ttl=ttl,
            description=description,
            name=name,
            meta=meta,
            status=JobStatus.QUEUED,
            timeout=timeout,
            retries_left=retries_left,
            retry_intervals=retry_intervals,
            on_success=on_success,
            on_failure=on_failure,
            on_stopped=on_stopped,
            queue_name=self.name,
            task_type=task_type,
            scheduled_task_id=scheduled_task_id,
        )
        job_model = self._enqueue_job(job_model, connection=pipeline, at_front=at_front)
        return job_model

    def run_job(self, job_model: JobModel) -> Any:
        """Run the job
        :param job_model: The job to run
        :returns: The job result
        """
        result = perform_job(job_model, self.connection)
        result_ttl = job_model.result_ttl or SCHEDULER_CONFIG.DEFAULT_RESULT_TTL
        with self.connection.pipeline() as pipeline:
            self.job_handle_success(job_model, result=result, result_ttl=result_ttl, connection=pipeline)
            job_model.expire(result_ttl, connection=pipeline)
            pipeline.execute()
        return result

    def job_handle_success(self, job: JobModel, result: Any, result_ttl: int, connection: ConnectionType):
        """Saves and cleanup job after successful execution"""
        job.set_status(JobStatus.FINISHED, connection=connection)
        job.save(connection=connection)
        Result.create(connection, job_name=job.name, _type=ResultType.SUCCESSFUL, return_value=result, ttl=result_ttl)

        if result_ttl != 0:
            self.finished_job_registry.add(connection, job.name, current_timestamp() + result_ttl)

    def job_handle_failure(self, job: JobModel, exc_string: str, connection: ConnectionType):
        # Does not set job status since the job might be stopped
        score = current_timestamp() + SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL
        self.failed_job_registry.add(connection, job.name, score)
        Result.create(connection, job.name, ResultType.FAILED, SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL,
                      exc_string=exc_string)

    def enqueue_at(self, when: datetime, f, *args, **kwargs) -> JobModel:
        """Schedules a job to be enqueued at specified time
        :param when: The time to enqueue the job
        :param f: The function to call
        :param args: The `*args` to pass to the function
        :param kwargs: The `**kwargs` to pass to the function
        :returns: The enqueued Job
        """
        job_model = JobModel.create(
            connection=self.connection,
            queue_name=self.name,
            func=f,
            status=JobStatus.SCHEDULED,
            *args, **kwargs
        )
        job_model.save(connection=self.connection)
        self.scheduled_job_registry.schedule(self.connection, job_model, when)
        return job_model

    def retry_job(self, job: JobModel, connection: ConnectionType):
        """Requeue or schedule this job for execution.
        If the the `retry_interval` was set on the job itself,
        it will calculate a scheduled time for the job to run, and instead
        of just regularly `enqueing` the job, it will `schedule` it.

        Args:
            job (JobModel): The queue to retry the job on
            connection (ConnectionType): The Redis' pipeline to use
        """
        number_of_intervals = len(job.retry_intervals)
        index = max(number_of_intervals - job.retries_left, 0)
        retry_interval = job.retry_intervals[index]
        job.retries_left = job.retries_left - 1
        if retry_interval:
            scheduled_datetime = utcnow() + timedelta(seconds=retry_interval)
            job.set_status(JobStatus.SCHEDULED, connection=connection)
            job.save(connection=connection)
            self.scheduled_job_registry.schedule(connection, job, scheduled_datetime)
        else:
            self._enqueue_job(job, connection=connection)

    def _enqueue_job(
            self,
            job_model: JobModel,
            connection: Optional[ConnectionType] = None,
            at_front: bool = False) -> JobModel:
        """Enqueues a job for delayed execution without checking dependencies.

        If Queue is instantiated with is_async=False, job is executed immediately.
        :param job_model: The job redis model
        :param connection: The Redis Pipeline
        :param at_front: Whether to enqueue the job at the front

        :returns: The enqueued JobModel
        """

        pipe = connection if connection is not None else self.connection.pipeline()

        # Add Queue key set
        job_model.status = JobStatus.QUEUED
        job_model.enqueued_at = utcnow()
        job_model.save(connection=pipe)
        job_model.expire(ttl=job_model.ttl, connection=pipe)

        if self._is_async:
            if at_front:
                score = current_timestamp()
            else:
                score = self.queued_job_registry.get_last_timestamp() or current_timestamp()
            self.queued_job_registry.add(connection=pipe, score=score, member=job_model.name)
            result = pipe.execute()
            logger.debug(f"Pushed job {job_model.name} into {self.name}, {result[3]} job(s) are in queue.")
        else:  # sync mode
            job_model = self.run_sync(job_model)

        return job_model

    def run_sync(self, job: JobModel) -> JobModel:
        """Run a job synchronously, meaning on the same process the method was called."""
        job.prepare_for_execution("sync", self.started_job_registry, self.connection)

        try:
            result = self.run_job(job)
        except:  # noqa
            with self.connection.pipeline() as pipeline:
                job.set_status(JobStatus.FAILED, connection=pipeline)
                exc_string = "".join(traceback.format_exception(*sys.exc_info()))
                self.job_handle_failure(job, exc_string, pipeline)
                pipeline.execute()

            if job.failure_callback:
                job.failure_callback(job, self.connection, *sys.exc_info())  # type: ignore
        else:
            if job.success_callback:
                job.success_callback(job, self.connection, result)  # type: ignore

        return job

    @classmethod
    def dequeue_any(
            cls,
            queues: List[Self],
            timeout: Optional[int],
            connection: Optional[ConnectionType] = None,
    ) -> Tuple[Optional[JobModel], Optional[Self]]:
        """Class method returning a Job instance at the front of the given set of Queues, where the order of the queues
        is important.

        When all the Queues are empty, depending on the `timeout` argument, either blocks execution of this function
        for the duration of the timeout or until new messages arrive on any of the queues, or returns None.

        :param queues: List of Queue objects
        :param timeout: Timeout for the pop operation
        :param connection: Broker Connection
        :returns: Tuple of Job, Queue
        """

        while True:
            registries = [q.queued_job_registry for q in queues]

            result = QueuedJobRegistry.pop(connection, registries, timeout)
            if result == (None, None):
                return None, None

            registry_key, job_name = map(as_str, result)
            queue = next(filter(lambda q: q.queued_job_registry.key == registry_key, queues), None)
            if queue is None:
                logger.warning(f"Could not find queue for registry key {registry_key} in queues")
                return None, None

            job = JobModel.get(job_name, connection=connection)
            if job is None:
                continue
            return job, queue
        return None, None

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, Queue):
            raise TypeError("Cannot compare queues to other objects")
        return self.name == other.name

    def __lt__(self, other: Self) -> bool:
        if not isinstance(other, Queue):
            raise TypeError("Cannot compare queues to other objects")
        return self.name < other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return "{0}({1!r})".format(self.__class__.__name__, self.name)

    def __str__(self) -> str:
        return "<{0} {1}>".format(self.__class__.__name__, self.name)

    def _remove_from_registries(self, job_name: str, connection: ConnectionType) -> None:
        """Removes the job from all registries besides failed_job_registry"""
        self.finished_job_registry.delete(connection=connection, job_name=job_name)
        self.scheduled_job_registry.delete(connection=connection, job_name=job_name)
        self.started_job_registry.delete(connection=connection, job_name=job_name)
        self.canceled_job_registry.delete(connection=connection, job_name=job_name)
        self.queued_job_registry.delete(connection=connection, job_name=job_name)

    def cancel_job(self, job_name: str) -> None:
        """Cancels the given job, which will prevent the job from ever running (or inspected).

        This method merely exists as a high-level API call to cancel jobs without worrying about the internals required
        to implement job cancellation.

        :param job_name: The job name to cancel.
        :raises NoSuchJobError: If the job does not exist.
        :raises InvalidJobOperation: If the job has already been canceled.
        """
        job = JobModel.get(job_name, connection=self.connection)
        if job is None:
            raise NoSuchJobError("No such job: {}".format(job_name))
        if job.status == JobStatus.CANCELED:
            raise InvalidJobOperation("Cannot cancel already canceled job: {}".format(job.name))

        pipe = self.connection.pipeline()

        while True:
            try:
                job.set_field("status", JobStatus.CANCELED, connection=pipe)
                self._remove_from_registries(job_name, connection=pipe)
                self.canceled_job_registry.add(pipe, job_name, 0)
                pipe.execute()
                break
            except WatchError:
                # if the pipeline comes from the caller, we re-raise the exception as it is the responsibility of the
                # caller to handle it
                raise

    def delete_job(self, job_name: str):
        """Deletes the given job from the queue and all its registries"""

        pipe = self.connection.pipeline()

        while True:
            try:
                self._remove_from_registries(job_name, connection=pipe)
                self.failed_job_registry.delete(connection=pipe, job_name=job_name)
                if JobModel.exists(job_name, connection=self.connection):
                    JobModel.delete_many([job_name], connection=pipe)
                pipe.execute()
                break
            except WatchError:
                pass

    def requeue_jobs(self, *job_names: str, at_front: bool = False) -> int:
        jobs = JobModel.get_many(job_names, connection=self.connection)
        jobs_requeued = 0
        with self.connection.pipeline() as pipe:
            for job in jobs:
                if job is None:
                    continue
                job.started_at = None
                job.ended_at = None
                job.save(connection=pipe)
                self._enqueue_job(job, connection=pipe, at_front=at_front)
                jobs_requeued += 1
            pipe.execute()
        return jobs_requeued
