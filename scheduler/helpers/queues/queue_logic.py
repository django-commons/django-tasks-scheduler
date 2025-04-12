import asyncio
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Any

from redis import WatchError

from scheduler.helpers.callback import Callback
from scheduler.helpers.utils import utcnow, current_timestamp
from scheduler.redis_models import (
    JobNamesRegistry,
    FinishedJobRegistry,
    ActiveJobRegistry,
    FailedJobRegistry,
    CanceledJobRegistry,
    ScheduledJobRegistry,
    QueuedJobRegistry,
)
from scheduler.redis_models import JobStatus, SchedulerLock, Result, ResultType, JobModel
from scheduler.settings import logger, SCHEDULER_CONFIG
from scheduler.types import ConnectionType, FunctionReferenceType, Self


class InvalidJobOperation(Exception):
    pass


class NoSuchJobError(Exception):
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
        if job_model.success_callback:
            job_model.success_callback(job_model, connection, result)  # type: ignore
        return result
    except:
        if job_model.failure_callback:
            job_model.failure_callback(job_model, connection, *sys.exc_info())  # type: ignore
        raise
    finally:
        assert job_model is _job_stack.pop()


_job_stack = []


class Queue:
    REGISTRIES = dict(
        finished="finished_job_registry",
        failed="failed_job_registry",
        scheduled="scheduled_job_registry",
        active="active_job_registry",
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
        self.active_job_registry = ActiveJobRegistry(connection=self.connection, name=self.name)
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
        self.queued_job_registry.compact()
        started_jobs: List[Tuple[str, float]] = self.active_job_registry.get_job_names_before(
            self.connection, before_score
        )

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

                else:
                    logger.warning(
                        f"Queue cleanup: Moving job to {self.failed_job_registry.key} (due to AbandonedJobError)"
                    )
                    exc_string = (
                        f"Moved to {self.failed_job_registry.key}, due to AbandonedJobError, at {datetime.now()}"
                    )
                    job.status = JobStatus.FAILED
                    score = current_timestamp() + SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL
                    Result.create(
                        connection=pipeline,
                        job_name=job.name,
                        worker_name=job.worker_name,
                        _type=ResultType.FAILED,
                        ttl=SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL,
                        exc_string=exc_string,
                    )
                    self.failed_job_registry.add(pipeline, job.name, score)
                    job.expire(connection=pipeline, ttl=SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL)
                    job.save(connection=pipeline)

            for registry in self.REGISTRIES.values():
                getattr(self, registry).cleanup(connection=self.connection, timestamp=before_score)
            pipeline.execute()

    def first_queued_job_name(self) -> Optional[str]:
        return self.queued_job_registry.get_first()

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

    def get_all_job_names(self) -> List[str]:
        res = list()
        res.extend(self.queued_job_registry.all())
        res.extend(self.finished_job_registry.all())
        res.extend(self.active_job_registry.all())
        res.extend(self.failed_job_registry.all())
        res.extend(self.scheduled_job_registry.all())
        res.extend(self.canceled_job_registry.all())
        return res

    def get_all_jobs(self) -> List[JobModel]:
        job_names = self.get_all_job_names()
        return JobModel.get_many(job_names, connection=self.connection)

    def create_and_enqueue_job(
        self,
        func: FunctionReferenceType,
        args: Union[Tuple, List, None] = None,
        kwargs: Optional[Dict] = None,
        when: Optional[datetime] = None,
        timeout: Optional[int] = None,
        result_ttl: Optional[int] = None,
        job_info_ttl: Optional[int] = None,
        description: Optional[str] = None,
        name: Optional[str] = None,
        at_front: bool = False,
        meta: Optional[Dict] = None,
        on_success: Optional[Callback] = None,
        on_failure: Optional[Callback] = None,
        on_stopped: Optional[Callback] = None,
        task_type: Optional[str] = None,
        scheduled_task_id: Optional[int] = None,
        pipeline: Optional[ConnectionType] = None,
    ) -> JobModel:
        """Creates a job to represent the delayed function call and enqueues it.
        :param when: When to schedule the job (None to enqueue immediately)
        :param func: The reference to the function
        :param args: The `*args` to pass to the function
        :param kwargs: The `**kwargs` to pass to the function
        :param timeout: Function timeout
        :param result_ttl: Result time to live
        :param job_info_ttl: Time to live
        :param description: The job description
        :param name: The job name
        :param at_front: Whether to enqueue the job at the front
        :param meta: Metadata to attach to the job
        :param on_success: Callback for on success
        :param on_failure: Callback for on failure
        :param on_stopped: Callback for on stopped
        :param task_type: The task type
        :param scheduled_task_id: The scheduled task id
        :param pipeline: The Broker Pipeline
        :returns: The enqueued Job
        """
        status = JobStatus.QUEUED if when is None else JobStatus.SCHEDULED
        job_model = JobModel.create(
            connection=self.connection,
            func=func,
            args=args,
            kwargs=kwargs,
            result_ttl=result_ttl,
            job_info_ttl=job_info_ttl,
            description=description,
            name=name,
            meta=meta,
            status=status,
            timeout=timeout,
            on_success=on_success,
            on_failure=on_failure,
            on_stopped=on_stopped,
            queue_name=self.name,
            task_type=task_type,
            scheduled_task_id=scheduled_task_id,
        )
        if when is None:
            job_model = self.enqueue_job(job_model, connection=pipeline, at_front=at_front)
        elif isinstance(when, datetime):
            job_model.save(connection=self.connection)
            self.scheduled_job_registry.schedule(self.connection, job_model.name, when)
        else:
            raise TypeError(f"Invalid type for when=`{when}`")
        return job_model

    def job_handle_success(
        self, job: JobModel, result: Any, job_info_ttl: int, result_ttl: int, connection: ConnectionType
    ):
        """Saves and cleanup job after successful execution"""
        job.after_execution(
            job_info_ttl,
            JobStatus.FINISHED,
            prev_registry=self.active_job_registry,
            new_registry=self.finished_job_registry,
            connection=connection,
        )
        Result.create(
            connection,
            job_name=job.name,
            worker_name=job.worker_name,
            _type=ResultType.SUCCESSFUL,
            return_value=result,
            ttl=result_ttl,
        )

    def job_handle_failure(self, status: JobStatus, job: JobModel, exc_string: str, connection: ConnectionType):
        # Does not set job status since the job might be stopped
        job.after_execution(
            SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL,
            status,
            prev_registry=self.active_job_registry,
            new_registry=self.failed_job_registry,
            connection=connection,
        )
        Result.create(
            connection,
            job.name,
            job.worker_name,
            ResultType.FAILED,
            SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL,
            exc_string=exc_string,
        )

    def run_sync(self, job: JobModel) -> JobModel:
        """Run a job synchronously, meaning on the same process the method was called."""
        job.prepare_for_execution("sync", self.active_job_registry, self.connection)
        try:
            result = perform_job(job, self.connection)

            with self.connection.pipeline() as pipeline:
                self.job_handle_success(
                    job, result=result, job_info_ttl=job.job_info_ttl, result_ttl=job.success_ttl, connection=pipeline
                )

                pipeline.execute()
        except Exception as e:  # noqa
            logger.warning(f"Job {job.name} failed with exception: {e}")
            with self.connection.pipeline() as pipeline:
                exc_string = "".join(traceback.format_exception(*sys.exc_info()))
                self.job_handle_failure(JobStatus.FAILED, job, exc_string, pipeline)
                pipeline.execute()
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
            for registry in registries:
                registry.compact()

            registry_key, job_name = QueuedJobRegistry.pop(connection, registries, timeout)
            if job_name is None:
                return None, None

            queue = next(filter(lambda q: q.queued_job_registry.key == registry_key, queues), None)
            if queue is None:
                logger.warning(f"Could not find queue for registry key {registry_key} in queues")
                return None, None

            job = JobModel.get(job_name, connection=connection)
            if job is None:
                continue
            return job, queue
        return None, None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} {self.name}>"

    def _remove_from_registries(self, job_name: str, connection: ConnectionType) -> None:
        """Removes the job from all registries besides failed_job_registry"""
        self.finished_job_registry.delete(connection=connection, job_name=job_name)
        self.scheduled_job_registry.delete(connection=connection, job_name=job_name)
        self.active_job_registry.delete(connection=connection, job_name=job_name)
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
            raise NoSuchJobError(f"No such job: {job_name}")
        if job.status == JobStatus.CANCELED:
            raise InvalidJobOperation(f"Cannot cancel already canceled job: {job.name}")

        pipe = self.connection.pipeline()
        new_status = JobStatus.CANCELED if job.status == JobStatus.QUEUED else JobStatus.STOPPED

        while True:
            try:
                job.set_field("status", new_status, connection=pipe)
                self._remove_from_registries(job_name, connection=pipe)
                pipe.execute()
                if new_status == JobStatus.CANCELED:
                    self.canceled_job_registry.add(pipe, job_name, 0)
                else:
                    self.finished_job_registry.add(
                        pipe, job_name, current_timestamp() + SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL
                    )
                pipe.execute()
                break
            except WatchError:
                # if the pipeline comes from the caller, we re-raise the exception as it is the responsibility of the
                # caller to handle it
                raise

    def delete_job(self, job_name: str, expire_job_model: bool = True) -> None:
        """Deletes the given job from the queue and all its registries"""
        pipe = self.connection.pipeline()

        while True:
            try:
                self._remove_from_registries(job_name, connection=pipe)
                self.failed_job_registry.delete(connection=pipe, job_name=job_name)
                if expire_job_model:
                    job_model = JobModel.get(job_name, connection=self.connection)
                    if job_model is not None:
                        job_model.expire(ttl=job_model.job_info_ttl, connection=pipe)
                pipe.execute()
                break
            except WatchError:
                pass

    def enqueue_job(
        self, job_model: JobModel, connection: Optional[ConnectionType] = None, at_front: bool = False
    ) -> JobModel:
        """Enqueues a job for delayed execution without checking dependencies.

        If Queue is instantiated with is_async=False, job is executed immediately.
        :param job_model: The job redis model
        :param connection: The Redis Pipeline
        :param at_front: Whether to enqueue the job at the front

        :returns: The enqueued JobModel
        """

        pipe = connection if connection is not None else self.connection.pipeline()
        job_model.started_at = None
        job_model.ended_at = None
        job_model.status = JobStatus.QUEUED
        job_model.enqueued_at = utcnow()
        job_model.save(connection=pipe)

        if self._is_async:
            if at_front:
                score = current_timestamp()
            else:
                score = self.queued_job_registry.get_last_timestamp() or current_timestamp()
            self.scheduled_job_registry.delete(connection=pipe, job_name=job_model.name)
            self.queued_job_registry.add(connection=pipe, score=score, job_name=job_model.name)
            pipe.execute()
            logger.debug(f"Pushed job {job_model.name} into {self.name} queued-jobs registry")
        else:  # sync mode
            pipe.execute()
            job_model = self.run_sync(job_model)
            job_model.expire(ttl=job_model.job_info_ttl, connection=pipe)
            pipe.execute()

        return job_model
