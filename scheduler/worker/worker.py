import contextlib
import errno
import math
import os
import random
import signal
import socket
import sys
import threading
import time
import traceback
import warnings
from datetime import timedelta
from enum import Enum
from logging import DEBUG, INFO, ERROR, WARNING
from random import shuffle
from types import FrameType
from typing import List, Optional, Tuple, Any, Iterable, Collection, Union

import scheduler
from scheduler.helpers.queues import get_queue
from scheduler.redis_models import WorkerModel, JobModel, JobStatus, DequeueTimeout
from scheduler.settings import SCHEDULER_CONFIG, logger, get_queue_configuration
from scheduler.types import Broker, Self
from scheduler.types import ConnectionType, TimeoutErrorTypes, ConnectionErrorTypes, WatchErrorTypes, ResponseErrorTypes
from .commands import WorkerCommandsChannelListener
from .scheduler import WorkerScheduler, SchedulerStatus
from ..helpers.queues.getters import get_queue_connection
from ..redis_models.lock import QueueLock
from ..redis_models.worker import WorkerStatus

try:
    from signal import SIGKILL
except ImportError:
    from signal import SIGTERM as SIGKILL  # type:ignore

from scheduler.helpers.queues import Queue, queue_perform_job
from scheduler.helpers.timeouts import JobExecutionMonitorTimeoutException, JobTimeoutException
from scheduler.helpers.utils import utcnow

try:
    from setproctitle import setproctitle as setprocname
except ImportError:

    def setprocname(*args: Any, **kwargs: Any) -> None:  # noqa
        pass


class StopRequested(Exception):
    pass


class WorkerNotFound(Exception):
    pass


class QueueConnectionDiscrepancyError(Exception):
    pass


_signames = dict(
    (getattr(signal, signame), signame) for signame in dir(signal) if signame.startswith("SIG") and "_" not in signame
)


def signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except KeyError:
        return "SIG_UNKNOWN"
    except ValueError:
        return "SIG_UNKNOWN"


class DequeueStrategy(str, Enum):
    DEFAULT = "default"
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"


class Worker:
    queue_class = Queue

    # factor to increase connection_wait_time in case of continuous connection failures.
    exponential_backoff_factor = 2.0
    # Max Wait time (in seconds) after which exponential_backoff_factor won't be applicable.
    max_connection_wait_time = 60.0

    @classmethod
    def from_model(cls, model: WorkerModel) -> Self:
        res = cls(
            queues=[get_queue(queue_name) for queue_name in model.queue_names],
            name=model.name,
            with_scheduler=False,
            model=model,
        )
        return res

    def __init__(
        self,
        queues: Iterable[Union[str, Queue]],
        name: str,
        maintenance_interval: int = SCHEDULER_CONFIG.DEFAULT_MAINTENANCE_TASK_INTERVAL,
        job_monitoring_interval: int = SCHEDULER_CONFIG.DEFAULT_JOB_MONITORING_INTERVAL,
        dequeue_strategy: DequeueStrategy = DequeueStrategy.DEFAULT,
        fork_job_execution: bool = True,
        with_scheduler: bool = True,
        burst: bool = False,
        model: Optional[WorkerModel] = None,
    ) -> None:
        self.fork_job_execution = fork_job_execution
        self.job_monitoring_interval: int = job_monitoring_interval
        self.maintenance_interval = maintenance_interval
        self.queues = [(get_queue(q) if isinstance(q, str) else q) for q in _ensure_list(queues)]
        self.name: str = name
        self._ordered_queues = self.queues[:]
        self._is_job_execution_process: bool = False
        self.scheduler: Optional[WorkerScheduler] = None
        self._command_listener = WorkerCommandsChannelListener(self.connection, self.name)
        self._dequeue_strategy = dequeue_strategy
        self.with_scheduler = with_scheduler
        self.burst = burst
        if model is None:
            self._validate_name_uniqueness()
            self._model = WorkerModel(
                name=self.name,
                queue_names=[queue.name for queue in self.queues],
                birth=None,
                last_heartbeat=None,
                pid=os.getpid(),
                hostname=socket.gethostname(),
                ip_address=_get_ip_address_from_connection(self.connection, self.name),
                version=scheduler.__version__,
                python_version=sys.version,
                state=WorkerStatus.CREATED,
            )
        else:
            self._model = model
        self._model.save(self.connection, save_all=True)

    @property
    def _pid(self) -> int:
        return self._model.pid

    @property
    def connection(self) -> ConnectionType:
        """Configures the Broker connection to have a socket timeout.
        This should timouet the connection in case any specific command hangs at any given time (eg. BLPOP).
        If the connection provided already has a `socket_timeout` defined, skips.
        """
        connection = get_queue_connection(self.queues[0].name)
        current_socket_timeout = connection.connection_pool.connection_kwargs.get("socket_timeout")
        if current_socket_timeout is None:
            timeout_config = {"socket_timeout": SCHEDULER_CONFIG.DEFAULT_WORKER_TTL - 5}
            connection.connection_pool.connection_kwargs.update(timeout_config)
        return connection

    def log(self, level: int, message: str, *args, **kwargs) -> None:
        logger.log(level, f"[Worker {self.name}/{self._pid}]: {message}", *args, **kwargs)

    def should_run_maintenance_tasks(self) -> bool:
        """Maintenance tasks should run on first startup or every 10 minutes."""
        if self._model.last_cleaned_at is None:
            return True
        if (utcnow() - self._model.last_cleaned_at) > timedelta(seconds=self.maintenance_interval):
            return True
        return False

    def clean_registries(self) -> None:
        """Runs maintenance jobs on each Queue's registries."""
        for queue in self.queues:
            # If there are multiple workers running, we only want 1 worker
            # to run clean_registries().
            queue_lock = QueueLock(self.name)
            if queue_lock.acquire(1, expire=899, connection=self.connection):
                self.log(DEBUG, f"Cleaning registries for queue: {queue.name}")
                queue.clean_registries()
                WorkerModel.cleanup(self.connection, queue.name)
                queue_lock.release(self.connection)
        self._model.last_cleaned_at = utcnow()

    def _install_signal_handlers(self) -> None:
        """Installs signal handlers for handling SIGINT and SIGTERM gracefully."""
        if threading.current_thread() is not threading.main_thread():
            self.log(DEBUG, "Running in a thread, skipping signal handlers installation")
            return
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def work(self, max_jobs: Optional[int] = None, max_idle_time: Optional[int] = None) -> bool:
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.
        If `max_idle_time` is provided, worker will die when it's idle for more than the provided value.

        The return value indicates whether any jobs were processed.

        :param max_jobs: Max number of jobs. Defaults to None.
        :param max_idle_time: Max seconds for a worker to be idle. Defaults to None.
        :return: Whether any jobs were processed.
        """
        self.bootstrap()

        self._install_signal_handlers()
        try:
            while True:
                self.refresh()
                self._check_for_suspension(self.burst)

                if self.should_run_maintenance_tasks():
                    self.run_maintenance_tasks()

                if self._model.shutdown_requested_date:
                    self.log(INFO, "stopping on request")
                    break

                timeout = None if self.burst else (SCHEDULER_CONFIG.DEFAULT_WORKER_TTL - 15)
                job, queue = self.dequeue_job_and_maintain_ttl(timeout, max_idle_time)
                if job is None or queue is None:
                    if self.burst:
                        self.log(INFO, "done, quitting")
                        break
                    elif max_idle_time is not None:
                        self.log(INFO, f"idle for {max_idle_time} seconds, quitting")
                        break
                    continue

                self.execute_job(job, queue)

                self.refresh()

                self._model.heartbeat(self.connection)
                self._model.save(self.connection)
                if max_jobs is not None and self._model.completed_jobs >= max_jobs:
                    self.log(INFO, f"finished executing {self._model.completed_jobs} jobs, quitting")
                    break
            return self._model.completed_jobs > 0

        except TimeoutErrorTypes:
            self.log(ERROR, "Redis connection timeout, quitting...")
        except StopRequested:
            self.log(INFO, "Worker was requested to stop, quitting")
        except SystemExit:  # Cold shutdown detected
            raise
        except Exception:
            self.log(ERROR, "found an unhandled exception, quitting...", exc_info=True)
        finally:
            self.teardown()
        return False

    def handle_job_failure(self, job: JobModel, queue: Queue, exc_string: str = "") -> None:
        """
        Handles the failure or an executing job by:
            1. Setting the job status to failed
            2. Removing the job from active_job_registry
            3. Setting the workers current job to None
            4. Add the job to FailedJobRegistry
        `save_exc_to_job` should only be used for testing purposes
        """
        self.log(DEBUG, f"Handling failed execution of job {job.name}")
        # check whether a job was stopped intentionally and set the job status appropriately if it was this job.
        stopped_job_name = self._model.get_field("stopped_job_name", self.connection)
        self._model.current_job_name = None
        if stopped_job_name == job.name:
            self.log(DEBUG, "Job was stopped, setting status to STOPPED")
            new_job_status = JobStatus.STOPPED
        else:
            self.log(DEBUG, "Job has failed, setting status to FAILED")
            new_job_status = JobStatus.FAILED

        queue.job_handle_failure(new_job_status, job, exc_string)

        self._model.current_job_working_time = 0
        if job.status == JobStatus.FAILED:
            self._model.failed_job_count += 1
            self._model.completed_jobs += 1
        if job.started_at is not None and job.ended_at is not None:
            self._model.total_working_time_ms += (job.ended_at - job.started_at).microseconds / 1000.0
        self._model.save(connection=self.connection)

    def bootstrap(self) -> None:
        """Bootstraps the worker.
        Runs the basic tasks that should run when the worker actually starts working.
        Used so that new workers can focus on the work loop implementation rather
        than the full bootstraping process.
        """
        self.worker_start()
        self.log(INFO, f"Worker {self.name} started with PID {os.getpid()}")
        self._command_listener.start()
        if self.with_scheduler:
            self.scheduler = WorkerScheduler(self.queues, worker_name=self.name)
            self.scheduler.start()
            self._model.has_scheduler = True
            self._model.save(connection=self.connection)
        if self.with_scheduler and self.burst:
            if self.scheduler is not None:
                self.scheduler.request_stop_and_wait()
            self._model.has_scheduler = False
            self._model.save(connection=self.connection)
        qnames = [queue.name for queue in self.queues]
        self.log(INFO, f"""Listening to queues {", ".join(qnames)}...""")

    def _check_for_suspension(self, burst: bool) -> None:
        """Check to see if workers have been suspended by `rq suspend`"""
        before_state = None
        notified = False
        while self._model.is_suspended:
            if burst:
                self.log(
                    INFO, "Suspended in burst mode, exiting, Note: There could still be unfinished jobs on the queue"
                )
                raise StopRequested()

            if not notified:
                self.log(INFO, "Worker suspended, trigger ResumeCommand")
                before_state = self._model.state
                self._model.set_field("state", WorkerStatus.SUSPENDED, connection=self.connection)
                notified = True
            time.sleep(1)

        if before_state:
            self._model.set_field("state", before_state, connection=self.connection)

    def run_maintenance_tasks(self) -> None:
        """Runs periodic maintenance tasks, these include:
        1. Check if scheduler should be started.
        2. Cleaning registries
        """
        self.clean_registries()
        if not self.with_scheduler:
            return
        if self.scheduler is None and self.with_scheduler:
            self.log(DEBUG, "Creating scheduler")
            self.scheduler = WorkerScheduler(self.queues, worker_name=self.name, connection=self.connection)
        if self.scheduler.status == SchedulerStatus.STOPPED:
            self.log(DEBUG, "Starting scheduler thread")
            self.scheduler.start()
            self._model.has_scheduler = True
        if self.burst:
            self.log(DEBUG, "Stopping scheduler thread (burst mode)")
            self.scheduler.request_stop_and_wait()
            self._model.has_scheduler = False
        self._model.save(connection=self.connection)

    def dequeue_job_and_maintain_ttl(
        self, timeout: Optional[int], max_idle_time: Optional[int] = None
    ) -> Tuple[Optional[JobModel], Optional[Queue]]:
        """Dequeues a job while maintaining the TTL.
        :param timeout: The timeout for the dequeue operation.
        :param max_idle_time: The maximum idle time for the worker.
        :returns: A tuple with the job and the queue.
        """
        qnames = ",".join([queue.name for queue in self.queues])

        self._model.set_field("state", WorkerStatus.IDLE, connection=self.connection)
        self._set_procline(f"Listening on {qnames}")
        self.log(DEBUG, f"listening on {qnames}...")
        connection_wait_time = 1.0
        idle_since = utcnow()
        idle_time_left = max_idle_time
        job, queue = None, None
        while True:
            try:
                self._model.heartbeat(self.connection)

                if self.should_run_maintenance_tasks():
                    self.run_maintenance_tasks()

                if timeout is not None and idle_time_left is not None:
                    timeout = min(timeout, idle_time_left)

                self.log(DEBUG, f"Fetching jobs on queues {qnames} and timeout {timeout}")
                job, queue = Queue.dequeue_any(self._ordered_queues, timeout, connection=self.connection)
                if job is not None and queue is not None:
                    self.reorder_queues(reference_queue=queue)
                    self.log(INFO, f"Popped job `{job.name}` from `{queue.name}`")
                break
            except DequeueTimeout:
                if max_idle_time is not None:
                    idle_for = (utcnow() - idle_since).total_seconds()
                    idle_time_left = math.ceil(max_idle_time - idle_for)
                    if idle_time_left <= 0:
                        break
            except ConnectionErrorTypes as conn_err:
                self.log(ERROR, f"Could not connect: {conn_err} Retrying in {connection_wait_time} seconds...")
                time.sleep(connection_wait_time)
                connection_wait_time *= self.exponential_backoff_factor
                connection_wait_time = min(connection_wait_time, self.max_connection_wait_time)

        self._model.heartbeat(self.connection)
        return job, queue

    def _set_procline(self, message: str) -> None:
        """Changes the current procname for the process.

        This can be used to make `ps -ef` output more readable.
        """
        setprocname(f"{self._model._key}: {message}")

    def _validate_name_uniqueness(self) -> None:
        """Validates that the worker name is unique."""
        worker_model = WorkerModel.get(self.name, connection=self.connection)
        if worker_model is not None and worker_model.death is None:
            raise ValueError(f"There exists an active worker named {self.name!r} already")

    def worker_start(self) -> None:
        """Registers its own birth."""
        self.log(DEBUG, "Registering birth")
        now = utcnow()
        self._model.birth = now
        self._model.last_heartbeat = now
        self._model.state = WorkerStatus.STARTED
        self._model.save(self.connection)

    def _kill_job_execution_process(self, sig: signal.Signals = SIGKILL) -> None:
        """Kill the job execution process but catch "No such process" error has the job execution process could already
        be dead.

        :param sig: Optional, Defaults to SIGKILL.
        """
        try:
            os.killpg(os.getpgid(self._model.job_execution_process_pid), sig)
            self.log(INFO, f"Killed job execution process pid {self._model.job_execution_process_pid}")
        except OSError as e:
            if e.errno != errno.ESRCH:  # "No such process" is fine with us
                raise
            self.log(DEBUG, "Job execution process already dead")

    def _wait_for_job_execution_process(self) -> Tuple[Optional[int], Optional[int]]:
        """Waits for the job execution process to complete.
        Uses `0` as argument as to include "any child in the process group of the current process".
        """
        pid = stat = None
        with contextlib.suppress(ChildProcessError):  # ChildProcessError: [Errno 10] No child processes
            pid, stat = os.waitpid(self._model.job_execution_process_pid, 0)
        return pid, stat

    def request_force_stop(self, signum: int, frame: Optional[FrameType]) -> None:
        """Terminates the application (cold shutdown).

        :param signum: Signal number
        :param frame: Frame
        :raises SystemExit: SystemExit
        """
        # When a worker is run through a worker pool, it may receive duplicate signals.
        # One is sent by the pool when it calls `pool.stop_worker()` and another is sent by the OS
        # when a user hits Ctrl+C. In this case, if we receive the second signal within 1 second, we ignore it.
        shutdown_date = self._model.shutdown_requested_date
        if shutdown_date is not None and (utcnow() - shutdown_date) < timedelta(seconds=1):
            self.log(DEBUG, "Shutdown signal ignored, received twice in less than 1 second")
            return

        self.log(WARNING, "Could shut down")

        # Take down the job execution process with the worker
        if self._model.job_execution_process_pid:
            self.log(DEBUG, f"Taking down job execution process {self._model.job_execution_process_pid} with me")
            self._kill_job_execution_process()
            self._wait_for_job_execution_process()
        raise SystemExit()

    def request_stop(self, signum: int, frame: Optional[FrameType]) -> None:
        """Stops the current worker loop but waits for child processes to end gracefully (warm shutdown).
        :param signum: Signal number
        :param frame: Frame
        """
        self.log(DEBUG, f"Got signal {signal_name(signum)}")
        self._model.set_field("shutdown_requested_date", utcnow(), self.connection)

        signal.signal(signal.SIGINT, self.request_force_stop)
        signal.signal(signal.SIGTERM, self.request_force_stop)

        self.log(INFO, "warm shut down requested")

        self.stop_scheduler()
        # If shutdown is requested in the middle of a job, wait until finish before shutting down and save the request.
        if self._model.state == WorkerStatus.BUSY:
            self._model.set_field("shutdown_requested_date", utcnow(), connection=self.connection)

            self.log(
                DEBUG,
                "Stopping after current job execution process is finished. Press Ctrl+C again for a cold shutdown.",
            )
        else:
            raise StopRequested()

    def reorder_queues(self, reference_queue: Queue) -> None:
        """Reorder the queues according to the strategy.
        As this can be defined both in the `Worker` initialization or in the `work` method,
        it doesn't take the strategy directly, but rather uses the private `_dequeue_strategy` attribute.

        :param reference_queue: The queues to reorder
        """
        if self._dequeue_strategy is None:
            self._dequeue_strategy = DequeueStrategy.DEFAULT

        if self._dequeue_strategy not in [e.value for e in DequeueStrategy]:
            raise ValueError(
                f"""[Worker {self.name}/{self._pid}]: Dequeue strategy should be one of {", ".join([e.value for e in DequeueStrategy])}"""
            )
        if self._dequeue_strategy == DequeueStrategy.DEFAULT:
            return
        if self._dequeue_strategy == DequeueStrategy.ROUND_ROBIN:
            pos = self._ordered_queues.index(reference_queue)
            self._ordered_queues = self._ordered_queues[pos + 1 :] + self._ordered_queues[: pos + 1]
            return
        if self._dequeue_strategy == DequeueStrategy.RANDOM:
            shuffle(self._ordered_queues)
            return

    def teardown(self) -> None:
        if self._is_job_execution_process:
            return
        self.stop_scheduler()
        self._command_listener.stop()
        self._model.delete(self.connection)

    def stop_scheduler(self) -> None:
        """Stop the scheduler thread.
        Will send the kill signal to the scheduler process,
        if there's an OSError, just passes and `join()`'s the scheduler process, waiting for the process to finish.
        """
        if self.scheduler is None:
            return
        self.log(INFO, f"Stopping scheduler thread {self.scheduler.pid}")
        self.scheduler.request_stop_and_wait()
        self.log(DEBUG, "Scheduler thread stopped")
        self.scheduler = None

    def refresh(self, update_queues: bool = False) -> None:
        """Refreshes the worker data.
        It will get the data from the datastore and update the Worker's attributes
        """
        self._model = WorkerModel.get(self.name, connection=self.connection)  # type:ignore
        if self._model is None:
            msg = f"Worker broker record for {self.name} not found, quitting..."
            self.log(ERROR, msg)
            raise WorkerNotFound(msg)
        if update_queues:
            self.queues = [Queue(name=queue_name, connection=self.connection) for queue_name in self._model.queue_names]

    def fork_job_execution_process(self, job: JobModel, queue: Queue) -> None:
        """Spawns a job execution process to perform the actual work and passes it a job.
        This is where the `fork()` actually happens.

        :param job: The job to be executed
        :param queue: The queue from which the job was dequeued
        """
        child_pid = os.fork()
        os.environ["SCHEDULER_WORKER_NAME"] = self.name
        os.environ["SCHEDULER_JOB_NAME"] = job.name
        if child_pid == 0:  # Child process/Job executor process to run the job
            os.setsid()
            worker = Worker.from_model(self._model)
            worker.execute_in_separate_process(job, queue)
            os._exit(0)  # just in case
        else:  # Parent worker process
            self.log(DEBUG, f"Forking job execution process, job_execution_process_pid={child_pid}")
            self._model.job_execution_process_pid = child_pid
            self._model.save(connection=self.connection)
            self._set_procline(f"Forked {child_pid} at {time.time()}")

    def get_heartbeat_ttl(self, job: JobModel) -> int:
        """Get's the TTL for the next heartbeat.
        :param job: The Job
        :return: The heartbeat TTL
        """
        if job.timeout and job.timeout > 0:
            remaining_execution_time = int(job.timeout - self._model.current_job_working_time)
            return min(remaining_execution_time, self.job_monitoring_interval) + 60
        else:
            return self.job_monitoring_interval + 60

    def monitor_job_execution_process(self, job: JobModel, queue: Queue) -> None:
        """The worker will monitor the job execution process and make sure that it either executes successfully or the
        status of the job is set to `failed`

        :param job: The Job
        :param queue: The Queue
        """
        retpid = ret_val = None
        while True:
            try:
                with SCHEDULER_CONFIG.DEATH_PENALTY_CLASS(
                    self.job_monitoring_interval, JobExecutionMonitorTimeoutException
                ):
                    retpid, ret_val = self._wait_for_job_execution_process()
                break
            except JobExecutionMonitorTimeoutException:
                # job execution process has not exited yet and is still running. Send a heartbeat to keep the worker alive.
                if job.started_at is not None:
                    working_time = (utcnow() - job.started_at).total_seconds()
                    self._model.set_current_job_working_time(working_time, self.connection)
                else:
                    self.log(WARNING, f"job {job.name} does not have started_at, cannot set working time")
                # Kill the job from this side if something is really wrong (interpreter lock/etc).
                if job.timeout != -1 and self._model.current_job_working_time > (job.timeout + 60):
                    self._model.heartbeat(self.connection, self.job_monitoring_interval + 60)
                    self._kill_job_execution_process()
                    self._wait_for_job_execution_process()
                    break

                self._model.heartbeat(self.connection, self.job_monitoring_interval + 60)

            except OSError as e:
                # In case we encountered an OSError due to EINTR (which is
                # caused by a SIGINT or SIGTERM signal during
                # os.waitpid()), we simply ignore it and enter the next
                # iteration of the loop, waiting for the child to end.  In
                # any other case, this is some other unexpected OS error,
                # which we don't want to catch, so we re-raise those ones.
                if e.errno != errno.EINTR:
                    raise
                # Send a heartbeat to keep the worker alive.
                self._model.heartbeat(self.connection)

        self.refresh()
        self._model.current_job_working_time = 0
        self._model.save(connection=self.connection)
        if ret_val == os.EX_OK:  # The process exited normally.
            return

        job_status = job.get_status(self.connection)
        stopped_job_name = self._model.get_field("stopped_job_name", self.connection)

        if job_status is None:
            self.log(WARNING, "Job status is None, completed and expired?")
            return
        elif stopped_job_name == job.name:  # job execution process killed deliberately
            self.log(WARNING, "Job stopped by user, moving job to failed-jobs-registry")
            job.call_stopped_callback()
            self.handle_job_failure(
                job, queue=queue, exc_string="Job stopped by user, job execution process terminated."
            )
        elif job_status not in [JobStatus.FINISHED, JobStatus.FAILED]:
            # Unhandled failure: move the job to the failed queue
            signal_msg = f" (signal {os.WTERMSIG(ret_val)})" if ret_val and os.WIFSIGNALED(ret_val) else ""
            exc_string = f"job-execution-process terminated unexpectedly; waitpid returned {ret_val}{signal_msg}; "
            self.log(WARNING, f"Moving job to {queue.name}/failed-job-registry ({exc_string})")

            self.handle_job_failure(job, queue=queue, exc_string=exc_string)

    def execute_job(self, job: JobModel, queue: Queue) -> None:
        """Spawns a job execution process to perform the actual work and passes it a job.
        The worker will wait for the job execution process and make sure it executes within the given timeout bounds, or
        will end the job execution process with SIGALRM.
        """
        if self.fork_job_execution:
            self._model.set_field("state", WorkerStatus.BUSY, connection=self.connection)
            self.fork_job_execution_process(job, queue)
            self.monitor_job_execution_process(job, queue)
            self._model.set_field("state", WorkerStatus.IDLE, connection=self.connection)
        else:
            self._model.set_field("state", WorkerStatus.BUSY, connection=self.connection)
            self.worker_perform_job(job, queue)
            self._model.set_field("state", WorkerStatus.IDLE, connection=self.connection)

    def execute_in_separate_process(self, job: JobModel, queue: Queue) -> None:
        """This is the entry point of the newly spawned job execution process.
        After fork()'ing, assure we are generating random sequences that are different from the worker.

        os._exit() is the way to exit from child processes after a fork(), in contrast to the regular sys.exit()
        """
        random.seed()
        self.setup_job_execution_process_signals()
        self._model.job_execution_process_pid = os.getpid()
        connection = self.connection
        self._model.save(connection=connection)
        self._is_job_execution_process = True
        job = JobModel.get(job.name, connection)
        try:
            self.worker_perform_job(job, queue)
        except:  # noqa
            os._exit(1)
        os._exit(0)

    def setup_job_execution_process_signals(self) -> None:
        """Setup signal handing for the newly spawned job execution process

        Always ignore Ctrl+C in the job execution process, as it might abort the currently running job.

        The main worker catches the Ctrl+C and requests graceful shutdown after the current work is done.
        When cold shutdown is requested, it kills the current job anyway.
        """
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def worker_before_execution(self, job: JobModel, connection: ConnectionType) -> None:
        self.log(DEBUG, f"Preparing for execution of job: `{job.name}`")
        current_pid = os.getpid()
        self._model.current_job_name = job.name
        self._model.current_job_working_time = 0
        self._model.job_execution_process_pid = current_pid
        heartbeat_ttl = self.get_heartbeat_ttl(job)
        self._model.heartbeat(connection, heartbeat_ttl)
        self._set_procline(f"Processing {job.func_name} from {job.queue_name} since {time.time()}")
        self._model.save(connection=connection)

    def handle_job_success(self, job: JobModel, return_value: Any, queue: Queue) -> None:
        """Handles the successful execution of certain job.
        It will remove the job from the `active_job_registry`, adding it to the `SuccessfulJobRegistry`,
        and run a few maintenance tasks including:
            - Resting the current job name
            - Enqueue dependents
            - Incrementing the job count and working time
            - Handling of the job successful execution

        Runs within a loop with the `watch` method so that protects interactions with dependents keys.

        :param job: The job that was successful.
        :param queue: The queue
        """
        self.log(DEBUG, f"Handling successful execution of job {job.name}")
        while True:
            try:
                connection = self.connection
                queue.job_handle_success(
                    job, result=return_value, job_info_ttl=job.job_info_ttl, result_ttl=job.success_ttl
                )
                self._model.current_job_name = None
                self._model.successful_job_count += 1
                self._model.completed_jobs += 1
                if job.started_at is not None and job.ended_at is not None:
                    self._model.total_working_time_ms += (job.ended_at - job.started_at).microseconds / 1000.0
                self._model.save(connection=connection)
                job.expire(job.success_ttl, connection=connection)
                self.log(DEBUG, f"Finished handling successful execution of job {job.name}")
                break
            except WatchErrorTypes:
                continue

    def worker_perform_job(self, job: JobModel, queue: Queue) -> bool:
        """Performs the actual work of a job.
        Called from the process executing the job (forked job execution process).

        :param job: The job to perform
        :param queue: The queue the job was dequeued from
        :returns: True after finished.
        """
        self.log(DEBUG, f"Performing {job.name} code.")

        try:
            connection = self.connection
            self.worker_before_execution(job, connection=connection)
            job.prepare_for_execution(self.name, queue.active_job_registry, connection=connection)
            timeout = job.timeout or SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT
            with SCHEDULER_CONFIG.DEATH_PENALTY_CLASS(timeout, JobTimeoutException, job_name=job.name):
                self.log(DEBUG, f"Performing job `{job.name}`...")
                rv = queue_perform_job(job, connection)
                self.log(DEBUG, f"Finished performing job `{job.name}`")

            self.handle_job_success(job=job, return_value=rv, queue=queue)
        except:  # NOQA
            self.log(DEBUG, f"Job {job.name} raised an exception.")
            exc_info = sys.exc_info()
            exc_string = "".join(traceback.format_exception(*exc_info))

            self.handle_job_failure(job=job, exc_string=exc_string, queue=queue)
            self.handle_exception(job, *exc_info)
            return False

        self.log(INFO, f"queue:{queue.name}/job:{job.name} performed.")
        self.log(DEBUG, f"job:{job.name} result: {str(rv)}")

        return True

    def handle_exception(self, job: JobModel, *exc_info: Any) -> None:
        """Walks the exception handler stack to delegate exception handling.
        If the job cannot be deserialized, it will raise when func_name or
        the other properties are accessed, which will stop exceptions from
        being properly logged, so we guard against it here.
        """
        logger.debug(f"Handling exception caused while performing job:{job.name}.")
        exc_string = "".join(traceback.format_exception(*exc_info))

        extra = {
            "func": job.func_name,
            "arguments": job.args,
            "kwargs": job.kwargs,
            Queue: job.queue_name,
            "job_name": job.name,
        }
        func_name = job.func_name

        # func_name
        self.log(ERROR, f"exception raised while executing ({func_name})\n{exc_string}", extra=extra)


class SimpleWorker(Worker):
    def execute_job(self, job: JobModel, queue: Queue) -> None:
        """Execute job in same thread/process, do not fork()"""
        self._model.set_field("state", WorkerStatus.BUSY, connection=self.connection)
        self.worker_perform_job(job, queue)
        self._model.set_field("state", WorkerStatus.IDLE, connection=self.connection)


class RoundRobinWorker(Worker):
    """Modified version of Worker that dequeues jobs from the queues using a round-robin strategy."""

    def reorder_queues(self, reference_queue: Queue) -> None:
        pos = self._ordered_queues.index(reference_queue)
        self._ordered_queues = self._ordered_queues[pos + 1 :] + self._ordered_queues[: pos + 1]


class RandomWorker(Worker):
    """Modified version of Worker that dequeues jobs from the queues using a random strategy."""

    def reorder_queues(self, reference_queue: Queue) -> None:
        shuffle(self._ordered_queues)


def _get_ip_address_from_connection(connection: ConnectionType, client_name: str) -> str:
    try:
        connection.client_setname(client_name)
    except ResponseErrorTypes:
        warnings.warn("CLIENT SETNAME command not supported, setting ip_address to unknown", Warning)
        return "unknown"
    client_list = connection.client_list()
    client_address_list: List[str] = [client["addr"] for client in client_list if client["name"] == client_name]
    if len(client_address_list) > 0:
        return client_address_list[0]
    else:
        warnings.warn("CLIENT LIST command not supported, setting ip_address to unknown", Warning)
        return "unknown"


def _ensure_list(obj: Any) -> List[Any]:
    """When passed an iterable of objects, does nothing, otherwise, it returns a list with just that object in it.

    :param obj: The object to ensure is a list
    :return:
    """
    is_nonstring_iterable = isinstance(obj, Iterable) and not isinstance(obj, str)
    return obj if is_nonstring_iterable else [obj]


def _calc_worker_name(existing_worker_names: Collection[str]) -> str:
    hostname = os.uname()[1]
    c = 1
    worker_name = f"{hostname}-worker.{c}"
    while worker_name in existing_worker_names:
        c += 1
        worker_name = f"{hostname}-worker.{c}"
    return worker_name


def get_queues(*queue_names: str) -> List[Queue]:
    """Return queue instances from specified queue names. All instances must use the same connection configuration."""

    queue_config = get_queue_configuration(queue_names[0])
    queues = [get_queue(queue_names[0])]
    # perform consistency checks while building return list
    for queue_name in queue_names[1:]:
        curr_queue_config = get_queue_configuration(queue_name)
        if not queue_config.same_connection_params(curr_queue_config):
            raise QueueConnectionDiscrepancyError(
                f'Queues must have the same broker connection. "{queue_name}" and "{queue_names[0]}" have different connection settings'
            )
        queue = get_queue(queue_name)
        queues.append(queue)

    return queues


def create_worker(*queue_names: str, **kwargs: Any) -> Worker:
    """Returns a Django worker for all queues or specified ones."""
    queues = get_queues(*queue_names)
    existing_worker_names: Collection[str] = WorkerModel.all_names(connection=queues[0].connection)
    kwargs.setdefault("fork_job_execution", SCHEDULER_CONFIG.BROKER != Broker.FAKEREDIS)
    if kwargs.get("name", None) is None:
        kwargs["name"] = _calc_worker_name(existing_worker_names)
    if kwargs["name"] in existing_worker_names:
        raise ValueError(f"Worker {kwargs['name']} already exists")
    kwargs["name"] = kwargs["name"].replace("/", ".")
    kwargs.setdefault("with_scheduler", False)
    worker = Worker(queues, **kwargs)
    return worker
