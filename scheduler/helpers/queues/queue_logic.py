import asyncio
import contextvars
import sys
import traceback
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any, ClassVar

from scheduler.helpers.callback import Callback
from scheduler.helpers.utils import current_timestamp, utcnow
from scheduler.redis_models import (
    ActiveJobRegistry,
    CanceledJobRegistry,
    FailedJobRegistry,
    FinishedJobRegistry,
    JobModel,
    JobNamesRegistry,
    JobStatus,
    QueuedJobRegistry,
    Result,
    ResultType,
    ScheduledJobRegistry,
    SchedulerLock,
    as_str,
)
from scheduler.settings import SCHEDULER_CONFIG, logger
from scheduler.types import ConnectionType, FunctionReferenceType, PipelineType, Self


class InvalidJobOperation(Exception):
    pass


class NoSuchJobError(Exception):
    pass


class NoSuchRegistryError(Exception):
    pass


_job_stack_var: contextvars.ContextVar[tuple[JobModel, ...]] = contextvars.ContextVar("_job_stack_var", default=())


def queue_perform_job(job_model: JobModel, connection: ConnectionType) -> Any:
    """The main execution method. Invokes the job function with the job arguments.

    :returns: The job's return value
    """
    job_model.persist(connection=connection)
    current_stack = _job_stack_var.get()
    token = _job_stack_var.set((*current_stack, job_model))

    try:
        result = job_model.func(*job_model.args, **job_model.kwargs)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)
        # Save once the job's code has run - for a coroutine that is only now - so `meta` changes it made are kept.
        job_model.save(connection=connection, save_all=True)
        job_model.call_success_callback(job_model, connection, result)
        return result
    except Exception as e:
        logger.error(f"Job {job_model.name} failed with exception: {e}", exc_info=True)
        job_model.call_failure_callback(job_model, connection, *sys.exc_info())
        raise
    finally:
        _job_stack_var.reset(token)


def get_current_job() -> JobModel | None:
    """Returns the job that is currently being executed, or ``None`` when called outside a job context.

    This is meant to be called from within a job's callable, e.g. to update ``job.meta`` to report progress.
    The job is persisted automatically once the callable returns, so mutating ``job.meta`` is enough::

        from scheduler.worker import get_current_job

        def my_task():
            job = get_current_job()
            job.meta["progress"] = 0.5
    """
    stack = _job_stack_var.get()
    return stack[-1] if stack else None


class Queue:
    REGISTRIES: ClassVar[dict[str, str]] = {
        "finished": "finished_job_registry",
        "failed": "failed_job_registry",
        "scheduled": "scheduled_job_registry",
        "active": "active_job_registry",
        "canceled": "canceled_job_registry",
        "queued": "queued_job_registry",
    }

    def __init__(self, connection: ConnectionType, name: str, is_async: bool = True) -> None:
        """Initializes a Queue object.

        :param name: The queue name
        :param connection: Broker connection
        :param is_async: Whether jobs should run "async" (using the worker).
        """
        self.connection: ConnectionType = connection
        self.name = name
        self._is_async = is_async
        self.queued_job_registry = QueuedJobRegistry(name=self.name)
        self.active_job_registry = ActiveJobRegistry(name=self.name)
        self.failed_job_registry = FailedJobRegistry(name=self.name)
        self.finished_job_registry = FinishedJobRegistry(name=self.name)
        self.scheduled_job_registry = ScheduledJobRegistry(name=self.name)
        self.canceled_job_registry = CanceledJobRegistry(name=self.name)

    def __len__(self) -> int:
        return self.count

    @property
    def scheduler_pid(self) -> int | None:
        lock = SchedulerLock(self.name)
        token = as_str(lock.value(self.connection))
        if token is None:
            return None
        # The token is `<pid>:<random>`; older versions stored the bare pid.
        return int(token.split(":", 1)[0])

    def clean_registries(self, timestamp: float | None = None) -> None:
        """Remove abandoned jobs from registry and add them to FailedJobRegistry.

        Removes jobs with an expiry time earlier than current_timestamp, specified as seconds since the Unix epoch.
        Removed jobs are added to the global failed job queue.
        """
        before_score = timestamp or current_timestamp()
        self.queued_job_registry.compact(self.connection)
        started_jobs: list[tuple[str, float]] = self.active_job_registry.get_job_names_before(
            self.connection, before_score
        )

        # `job_score` is already the job's expiry: `prepare_for_execution` scores the active registry
        # entry with `started_at + timeout`, and nothing refreshes it afterwards. So
        # `get_job_names_before` has done the whole comparison, and re-testing `job_score + timeout`
        # here would count the timeout twice, holding a job back until `started_at + 2 * timeout`.
        for job_name, _job_score in started_jobs:
            job = JobModel.get(job_name, connection=self.connection)
            if job is None:
                self.active_job_registry.delete(connection=self.connection, job_name=job_name)
                continue

            # Only the callback is conditional. Every abandoned job is moved to the failed registry,
            # which is what this method documents and what it did while `has_failure_callback` was a
            # bound method and the guard was therefore always true. Skipping the move for a job with
            # no failure callback would drop it from the active registry in the sweep below without
            # recording it anywhere, leaving `status=STARTED` and nothing visible in the admin.
            if job.has_failure_callback:
                logger.debug(f"Running failure callbacks for {job.name}")
                try:
                    job.call_failure_callback(job, self.connection, traceback.extract_stack())
                except Exception:
                    logger.exception(f"Job {self.name}: error while executing failure callback")
                    raise

            logger.warning(f"Queue cleanup: Moving job to {self.failed_job_registry.key} (due to AbandonedJobError)")
            exc_string = f"Moved to {self.failed_job_registry.key}, due to AbandonedJobError, at {utcnow()}"
            self.job_handle_failure(JobStatus.FAILED, job, exc_string)

        for registry in self.REGISTRIES.values():
            getattr(self, registry).cleanup(connection=self.connection, timestamp=before_score)

    def first_queued_job_name(self) -> str | None:
        return self.queued_job_registry.get_first(self.connection)

    @property
    def count(self) -> int:
        """Returns a count of all messages in the queue."""
        return sum(self.registry_counts().values())

    def registry_counts(self) -> dict[str, int]:
        """Returns the number of jobs in each registry, by registry name, cleaning them up first - in one round trip."""
        registries: list[JobNamesRegistry] = [getattr(self, attr) for attr in self.REGISTRIES.values()]
        timestamp = current_timestamp()
        with self.connection.pipeline() as pipeline:
            for registry in registries:
                registry.cleanup(pipeline, timestamp)  # some registries queue no command here
            for registry in registries:
                pipeline.zcard(registry.key)
            counts = pipeline.execute()[-len(registries) :]
        return dict(zip(self.REGISTRIES, counts))

    def get_registry(self, name: str) -> JobNamesRegistry:
        name = name.lower()
        if name in Queue.REGISTRIES:
            return getattr(self, Queue.REGISTRIES[name])  # type: ignore
        raise NoSuchRegistryError(f"Unknown registry name {name}")

    def get_all_job_names(self) -> list[str]:
        all_job_names = []
        all_job_names.extend(self.queued_job_registry.all(self.connection))
        all_job_names.extend(self.finished_job_registry.all(self.connection))
        all_job_names.extend(self.active_job_registry.all(self.connection))
        all_job_names.extend(self.failed_job_registry.all(self.connection))
        all_job_names.extend(self.scheduled_job_registry.all(self.connection))
        all_job_names.extend(self.canceled_job_registry.all(self.connection))
        found = JobModel.exists_many(all_job_names, self.connection)
        return [job_name for job_name, exists in zip(all_job_names, found) if exists]

    def pending_job_names(self, job_names: Iterable[str]) -> set[str]:
        """Returns those of `job_names` that are scheduled, queued or running, checking them all in one round trip."""
        job_names = list(job_names)
        if not job_names:
            return set()
        registries = (self.scheduled_job_registry, self.queued_job_registry, self.active_job_registry)
        with self.connection.pipeline() as pipeline:
            for job_name in job_names:
                for registry in registries:
                    registry.exists(pipeline, job_name)
            ranks = pipeline.execute()
        # One rank per (job, registry) pair, in order: a job is pending if it has a rank in any of the registries.
        n = len(registries)
        return {
            name for i, name in enumerate(job_names) if any(rank is not None for rank in ranks[i * n : (i + 1) * n])
        }

    def get_all_jobs(self) -> list[JobModel]:
        job_names = self.get_all_job_names()
        return JobModel.get_many(job_names, connection=self.connection)

    def create_and_enqueue_job(
        self,
        func: FunctionReferenceType,
        args: tuple[Any, ...] | list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        when: datetime | None = None,
        timeout: int | None = None,
        result_ttl: int | None = None,
        job_info_ttl: int | None = None,
        description: str | None = None,
        name: str | None = None,
        at_front: bool = False,
        meta: dict[str, Any] | None = None,
        on_success: Callback | None = None,
        on_failure: Callback | None = None,
        on_stopped: Callback | None = None,
        task_type: str | None = None,
        scheduled_task_id: int | None = None,
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
            job_model = self.enqueue_job(job_model, at_front=at_front)
        elif isinstance(when, datetime):
            job_model.save(connection=self.connection)
            self.scheduled_job_registry.schedule(self.connection, job_model.name, when)
        else:
            raise TypeError(f"Invalid type for when=`{when}`")
        return job_model

    def job_handle_success(self, job: JobModel, result: Any, job_info_ttl: int, result_ttl: int) -> None:
        """Saves and cleanup job after successful execution"""
        job.after_execution(
            job_info_ttl,
            JobStatus.FINISHED,
            prev_registry=self.active_job_registry,
            new_registry=self.finished_job_registry,
            connection=self.connection,
        )
        Result.create(
            self.connection,
            job_name=job.name,
            worker_name=job.worker_name,
            _type=ResultType.SUCCESSFUL,
            return_value=result,
            ttl=result_ttl,
        )

    def job_handle_failure(self, status: JobStatus, job: JobModel, exc_string: str) -> None:
        # Does not set job status since the job might be stopped
        job.after_execution(
            SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL,
            status,
            prev_registry=self.active_job_registry,
            new_registry=self.failed_job_registry,
            connection=self.connection,
        )
        Result.create(
            self.connection,
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
            result = queue_perform_job(job, self.connection)
            self.job_handle_success(job, result=result, job_info_ttl=job.job_info_ttl, result_ttl=job.success_ttl)
        except Exception as e:
            logger.warning(f"Job {job.name} failed with exception: {e}")
            exc_string = "".join(traceback.format_exception(*sys.exc_info()))
            self.job_handle_failure(JobStatus.FAILED, job, exc_string)
        return job

    @classmethod
    def dequeue_any(
        cls, queues: list[Self], timeout: int | None, connection: ConnectionType
    ) -> tuple[JobModel | None, Self | None]:
        """Class method returning a Job instance at the front of the given set of Queues, where the order of the queues
        is important.

        When all the Queues are empty, depending on the `timeout` argument, either blocks execution of this function
        for the duration of the timeout or until new messages arrive on any of the queues, or returns None.

        :param queues: List of Queue objects
        :param timeout: Timeout for the pop operation
        :param connection: Broker Connection
        :returns: Tuple of Job, Queue
        """

        registries = [q.queued_job_registry for q in queues]
        while True:
            # No compacting first: that checked every queued job on every dequeue. A popped job whose data has expired
            # is skipped below instead, and maintenance compacts the queues periodically.
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

        # A WatchError here is deliberately left to propagate: handling it is the caller's responsibility.
        job.set_field("status", new_status, connection=pipe)
        self._remove_from_registries(job_name, connection=pipe)
        pipe.execute()
        if new_status == JobStatus.CANCELED:
            self.canceled_job_registry.add(pipe, job_name, 0)
        else:
            self.finished_job_registry.add(pipe, job_name, current_timestamp() + SCHEDULER_CONFIG.DEFAULT_FAILURE_TTL)
        pipe.execute()

    def delete_jobs(self, job_names: Sequence[str], expire_job_model: bool = True) -> None:
        """Deletes the given jobs from the queue and all its registries in one pipeline."""
        if not job_names:
            return
        job_models = JobModel.get_many(job_names, connection=self.connection) if expire_job_model else []
        with self.connection.pipeline() as pipe:
            for job_name in job_names:
                self._remove_from_registries(job_name, connection=pipe)
                self.failed_job_registry.delete(connection=pipe, job_name=job_name)
            for job_model in job_models:
                if job_model is not None:
                    job_model.expire(ttl=job_model.job_info_ttl, connection=pipe)
            pipe.execute()

    def delete_job(self, job_name: str, expire_job_model: bool = True) -> None:
        """Deletes the given job from the queue and all its registries"""
        self.delete_jobs([job_name], expire_job_model=expire_job_model)

    def enqueue_job(
        self, job_model: JobModel, pipeline: PipelineType | None = None, at_front: bool = False
    ) -> JobModel:
        """Enqueues a job for delayed execution without checking dependencies.

        If Queue is instantiated with is_async=False, the job is executed immediately.
        :param job_model: The job redis model
        :param pipeline: The Broker Pipeline
        :param at_front: Should the job be enqueued at the front

        :returns: The enqueued JobModel
        """

        pipe: PipelineType = pipeline if pipeline is not None else self.connection.pipeline()
        job_model.started_at = None
        job_model.ended_at = None
        job_model.status = JobStatus.QUEUED
        job_model.enqueued_at = utcnow()
        job_model.save(connection=pipe)
        self.failed_job_registry.delete(connection=pipe, job_name=job_model.name)

        if self._is_async:
            if at_front:
                first = self.connection.zrange(self.queued_job_registry.key, 0, 0, withscores=True)
                score = int(first[0][1]) - 1 if first else current_timestamp()
            else:
                score = current_timestamp()
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
