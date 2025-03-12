import contextlib
import errno
import math
import os
import random
import signal
import socket
import sys
import time
import traceback
import warnings
from datetime import timedelta
from enum import Enum
from random import shuffle
from types import FrameType
from typing import Callable, List, Optional, Tuple, Any, Iterable
from uuid import uuid4

import scheduler
from scheduler.broker_types import (ConnectionType, TimeoutErrorType, ConnectionErrorTypes, WatchErrorType,
                                    ResponseErrorTypes)
from scheduler.helpers.queues import get_queue
from scheduler.redis_models import WorkerModel, JobModel, JobStatus, KvLock, DequeueTimeout
from scheduler.settings import SCHEDULER_CONFIG, logger
from .commands import WorkerCommandsChannelListener
from .scheduler import WorkerScheduler
from ..redis_models.worker import WorkerStatus

try:
    from signal import SIGKILL
except ImportError:
    from signal import SIGTERM as SIGKILL

from contextlib import suppress

from scheduler.helpers.queues import Queue, perform_job
from scheduler.helpers.timeouts import (
    JobExecutionMonitorTimeoutException,
    JobTimeoutException,
)
from scheduler.helpers.utils import utcnow, current_timestamp

try:
    from setproctitle import setproctitle as setprocname
except ImportError:

    def setprocname(*args, **kwargs):  # noqa
        pass


class StopRequested(Exception):
    pass


_signames = dict(
    (getattr(signal, signame), signame) for signame in dir(signal) if signame.startswith("SIG") and "_" not in signame
)


def signal_name(signum):
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


class QueueLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"_lock:queue:{queue_name}")


class Worker:
    queue_class = Queue

    # factor to increase connection_wait_time in case of continuous connection failures.
    exponential_backoff_factor = 2.0
    # Max Wait time (in seconds) after which exponential_backoff_factor won't be applicable.
    max_connection_wait_time = 60.0

    def __init__(
            self,
            queues,
            name: Optional[str] = None,
            connection: Optional[ConnectionType] = None,
            exception_handlers=None,
            maintenance_interval: int = SCHEDULER_CONFIG.DEFAULT_MAINTENANCE_TASK_INTERVAL,
            job_monitoring_interval=SCHEDULER_CONFIG.DEFAULT_JOB_MONITORING_INTERVAL,
            dequeue_strategy: DequeueStrategy = DequeueStrategy.DEFAULT,
            disable_default_exception_handler: bool = False,
            fork_job_execution: bool = True,
    ):  # noqa
        self.fork_job_execution = fork_job_execution
        self.job_monitoring_interval = job_monitoring_interval
        self.maintenance_interval = maintenance_interval

        connection = self._set_connection(connection)
        self.connection = connection

        self.version = scheduler.__version__
        self.python_version = sys.version

        self.queues = [
            (Queue(name=q, connection=connection) if isinstance(q, str) else q)
            for q in _ensure_list(queues)
        ]
        self._model: WorkerModel
        self.name: str = name or uuid4().hex
        self._ordered_queues = self.queues[:]
        self._exc_handlers: List[Callable] = []

        self._is_job_execution_process: bool = False
        self._stop_requested: bool = False

        self.scheduler: Optional[WorkerScheduler] = None
        self._command_listener = WorkerCommandsChannelListener(connection, self.name)
        self._dequeue_strategy = dequeue_strategy

        self.disable_default_exception_handler = disable_default_exception_handler
        self.hostname: Optional[str] = socket.gethostname()
        self.pid: Optional[int] = os.getpid()
        self.ip_address = _get_ip_address_from_connection(self.connection, self.name)

        if isinstance(exception_handlers, (list, tuple)):
            for handler in exception_handlers:
                self.push_exc_handler(handler)
        elif exception_handlers is not None:
            self.push_exc_handler(exception_handlers)

    @property
    def should_run_maintenance_tasks(self):
        """Maintenance tasks should run on first startup or every 10 minutes."""
        if self._model.last_cleaned_at is None:
            return True
        if (utcnow() - self._model.last_cleaned_at) > timedelta(seconds=self.maintenance_interval):
            return True
        return False

    def _set_connection(self, connection: ConnectionType) -> ConnectionType:
        """Configures the Broker connection to have a socket timeout.
        This should timouet the connection in case any specific command hangs at any given time (eg. BLPOP).
        If the connection provided already has a `socket_timeout` defined, skips.

        :param connection: Broker connection to configure.
        """
        current_socket_timeout = connection.connection_pool.connection_kwargs.get("socket_timeout")
        if current_socket_timeout is None:
            timeout_config = {"socket_timeout": self.connection_timeout}
            connection.connection_pool.connection_kwargs.update(timeout_config)
        return connection

    def clean_registries(self):
        """Runs maintenance jobs on each Queue's registries."""
        for queue in self.queues:
            # If there are multiple workers running, we only want 1 worker
            # to run clean_registries().
            queue_lock = QueueLock(self.name)
            if queue_lock.acquire(1, expire=899, connection=self.connection):
                logger.info(f"Cleaning registries for queue: {queue.name}")
                queue.clean_registries()
                WorkerModel.cleanup(queue.connection, queue.name)
                queue_lock.release(self.connection)
        self._model.last_cleaned_at = utcnow()

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM gracefully."""
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def work(
            self,
            burst: bool = False,
            max_jobs: Optional[int] = None,
            max_idle_time: Optional[int] = None,
            with_scheduler: bool = True,
    ) -> bool:
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.
        If `max_idle_time` is provided, worker will die when it's idle for more than the provided value.

        The return value indicates whether any jobs were processed.

        :param burst: Whether to work on burst mode. Defaults to False.
        :param max_jobs: Max number of jobs. Defaults to None.
        :param max_idle_time: Max seconds for a worker to be idle. Defaults to None.
        :param with_scheduler: Whether to run the scheduler in a separate process. Defaults to True.
        :return: Whether any jobs were processed.
        """
        self.bootstrap()
        if with_scheduler:
            self._start_scheduler(burst)

        self._install_signal_handlers()
        try:
            while True:
                self._check_for_suspension(burst)

                if self.should_run_maintenance_tasks:
                    self.run_maintenance_tasks()

                if self._stop_requested:
                    logger.info(f"Worker {self.name}: stopping on request")
                    break

                timeout = None if burst else (SCHEDULER_CONFIG.DEFAULT_WORKER_TTL - 15)
                job, queue = self.dequeue_job_and_maintain_ttl(timeout, max_idle_time)
                if job is None:
                    if burst:
                        logger.info(f"Worker {self.name}: done, quitting")
                    elif max_idle_time is not None:
                        logger.info(f"Worker {self.name}: idle for {max_idle_time} seconds, quitting")
                    break

                self.execute_job(job, queue)
                with self.connection.pipeline() as pipeline:
                    self._model.heartbeat(pipeline)
                    self._model.completed_jobs += 1
                    self._model.save(pipeline)
                    pipeline.execute()
                if max_jobs is not None and self._model.completed_jobs >= max_jobs:
                    logger.info(f"Worker {self.name}: finished executing {self._model.completed_jobs} jobs, quitting")
                    break

        except TimeoutErrorType:
            logger.error(f"Worker {self.name}: Redis connection timeout, quitting...")

        except StopRequested:
            logger.info(f"Worker {self.name}: Worker was requested to stop, quitting")
            pass

        except SystemExit:
            # Cold shutdown detected
            raise

        except Exception as e:
            logger.error(f"Worker {self.name}: Exception in work loop", exc_info=True)
            logger.error(f"Worker {self.name}: found an unhandled exception, quitting...", exc_info=True)
        finally:
            self.teardown()
        return bool(self._model.completed_jobs)

    def handle_job_failure(self, job: JobModel, queue: Queue, exc_string=""):
        """
        Handles the failure or an executing job by:
            1. Setting the job status to failed
            2. Removing the job from StartedJobRegistry
            3. Setting the workers current job to None
            4. Add the job to FailedJobRegistry
        `save_exc_to_job` should only be used for testing purposes
        """
        logger.debug(f"Handling failed execution of job {job.name}")
        job_is_stopped = self._model.get_field("stopped_job_name", self.connection) == job.name
        with self.connection.pipeline() as pipeline:

            # check whether a job was stopped intentionally and set the job
            # status appropriately if it was this job.
            retry = job.retries_left and job.retries_left > 0 and not job_is_stopped

            if job_is_stopped:
                job.set_status(JobStatus.STOPPED, connection=pipeline)
                self._model.set_field("stopped_job_name", None, self.connection)
            else:
                # Requeue/reschedule if retry is configured, otherwise
                if not retry:
                    job.set_status(JobStatus.FAILED, connection=pipeline)

            queue.started_job_registry.delete(connection=pipeline, job_name=job.name)

            if not self.disable_default_exception_handler and not retry:
                queue.job_handle_failure(job, exc_string, connection=pipeline)
                with suppress(ConnectionErrorTypes):
                    pipeline.execute()

            self._model.current_job_name = None
            self._model.current_job_working_time = 0
            self._model.failed_job_count += 1
            if job.started_at and job.ended_at:
                self._model.total_working_time += (job.ended_at - job.started_at).total_seconds()
            self._model.save(connection=pipeline)

            if retry:
                queue.retry_job(job, pipeline)

            try:
                pipeline.execute()
            except Exception:
                # Ensure that custom exception handlers are called even if the Broker is down
                pass

    def _start_scheduler(self, burst: bool = False):
        """Starts the scheduler process.
        This is specifically designed to be run by the worker when running the `work()` method.
        Instanciates the RQScheduler and tries to acquire a lock.
        If the lock is acquired, start scheduler.
        If worker is on burst mode just enqueues scheduled jobs and quits,
        otherwise, starts the scheduler in a separate process.

        Args:
            burst (bool, optional): Whether to work on burst mode. Defaults to False.
        """
        self.scheduler = WorkerScheduler(self.queues, connection=self.connection)
        self.scheduler.start(burst=burst)
        self._model.scheduler_pid = self.scheduler.pid

    def bootstrap(self, with_command_listener: bool = True):
        """Bootstraps the worker.
        Runs the basic tasks that should run when the worker actually starts working.
        Used so that new workers can focus on the work loop implementation rather
        than the full bootstraping process.
        """
        self.register_birth()
        logger.info(f"Worker {self.name} started with PID {os.getpid()}")
        if with_command_listener:
            self._command_listener.start()
        qnames = [queue.name for queue in self.queues]
        logger.info(f"""*** Listening to queues {', '.join(qnames)}...""")

    def _check_for_suspension(self, burst: bool) -> None:
        """Check to see if workers have been suspended by `rq suspend`"""
        before_state = None
        notified = False

        while not self._stop_requested and self._model.get_field("is_suspended", self.connection):
            if burst:
                logger.info("Suspended in burst mode, exiting")
                logger.info("Note: There could still be unfinished jobs on the queue")
                raise StopRequested

            if not notified:
                logger.info("Worker suspended, trigger ResumeCommand")
                before_state = self._model.state
                self._model.set_field("state", WorkerStatus.SUSPENDED, connection=self.connection)
                notified = True
            time.sleep(1)

        if before_state:
            self._model.set_field("state", before_state, connection=self.connection)

    def run_maintenance_tasks(self):
        """
        Runs periodic maintenance tasks, these include:
        1. Check if scheduler should be started. This check should not be run
           on first run since worker.work() already calls
           `scheduler.enqueue_scheduled_jobs()` on startup.
        2. Cleaning registries

        No need to try to start scheduler on first run
        """
        if self._model.last_cleaned_at and self.scheduler and not self.scheduler.pid:
            self.scheduler.start(burst=False)
        self.clean_registries()

    def dequeue_job_and_maintain_ttl(
            self, timeout: Optional[int], max_idle_time: Optional[int] = None
    ) -> Tuple[JobModel, Queue]:
        """Dequeues a job while maintaining the TTL.
        :param timeout: The timeout for the dequeue operation.
        :param max_idle_time: The maximum idle time for the worker.
        :returns: A tuple with the job and the queue.
        """
        result = None
        qnames = ",".join([queue.name for queue in self.queues])

        self._model.set_field("state", WorkerStatus.IDLE, connection=self.connection)
        self.procline(f"Listening on {qnames}")
        logger.debug(f"*** Listening on {qnames}...")
        connection_wait_time = 1.0
        idle_since = utcnow()
        idle_time_left = max_idle_time
        job, queue = None, None
        while True:
            try:
                self._model.heartbeat(self.connection)

                if self.should_run_maintenance_tasks:
                    self.run_maintenance_tasks()

                if timeout is not None and idle_time_left is not None:
                    timeout = min(timeout, idle_time_left)

                logger.debug(f"Dequeueing jobs on queues {qnames} and timeout {timeout}")
                job, queue = Queue.dequeue_any(self._ordered_queues, timeout, connection=self.connection)
                if job is not None:
                    self.reorder_queues(reference_queue=queue)
                    logger.debug(f"Dequeued job {job.name} from {queue.name}")
                    logger.info(f"{queue.name}: {job.name}")
                break
            except DequeueTimeout:
                if max_idle_time is not None:
                    idle_for = (utcnow() - idle_since).total_seconds()
                    idle_time_left = math.ceil(max_idle_time - idle_for)
                    if idle_time_left <= 0:
                        break
            except ConnectionErrorTypes as conn_err:
                logger.error(f"Could not connect to Broker: {conn_err} Retrying in {connection_wait_time} seconds...")
                time.sleep(connection_wait_time)
                connection_wait_time *= self.exponential_backoff_factor
                connection_wait_time = min(connection_wait_time, self.max_connection_wait_time)

        self._model.heartbeat(self.connection)
        return job, queue

    @property
    def connection_timeout(self) -> int:
        return SCHEDULER_CONFIG.DEFAULT_WORKER_TTL - 5

    def procline(self, message):
        """Changes the current procname for the process.

        This can be used to make `ps -ef` output more readable.
        """
        setprocname(f"{self._model._key}: {message}")

    def register_birth(self):
        """Registers its own birth."""
        logger.debug(f"Registering birth of worker {self.name}")
        worker_model = WorkerModel.get(self.name, connection=self.connection)
        if worker_model is not None and worker_model.get_field("death", self.connection) is None:
            raise ValueError(f"There exists an active worker named {self.name!r} already")
        now = utcnow()
        self._model = WorkerModel(
            name=self.name,
            queue_names=[queue.name for queue in self.queues],
            birth=now,
            last_heartbeat=now,
            pid=self.pid,
            hostname=self.hostname,
            ip_address=self.ip_address,
            version=self.version,
            python_version=self.python_version,
            state=WorkerStatus.STARTED,
        )
        self._model.save(self.connection)

    def kill_job_execution_process(self, sig: signal.Signals = SIGKILL):
        """Kill the job execution process but catch "No such process" error has the job execution process could already
        be dead.

        :param sig: Optional, Defaults to SIGKILL.
        """
        try:
            os.killpg(os.getpgid(self._model.job_execution_process_pid), sig)
            logger.info(f"Killed job execution process pid {self._model.job_execution_process_pid}")
        except OSError as e:
            if e.errno == errno.ESRCH:
                # "No such process" is fine with us
                logger.debug("Job execution process already dead")
            else:
                raise

    def wait_for_job_execution_process(self) -> Tuple[Optional[int], Optional[int], Optional["struct_rusage"]]:
        """Waits for the job execution process to complete.
        Uses `0` as argument as to include "any child in the process group of the current process".
        """
        pid = stat = rusage = None
        with contextlib.suppress(ChildProcessError):  # ChildProcessError: [Errno 10] No child processes
            pid, stat, rusage = os.wait4(self._model.job_execution_process_pid, 0)
        return pid, stat, rusage

    def request_force_stop(self, signum: int, frame: Optional[FrameType]):
        """Terminates the application (cold shutdown).

        :param signum: Signal number
        :param frame: Frame
        :raises SystemExit: SystemExit
        """
        # When a worker is run through a worker pool, it may receive duplicate signals.
        # One is sent by the pool when it calls `pool.stop_worker()` and another is sent by the OS
        # when a user hits Ctrl+C. In this case, if we receive the second signal within 1 second, we ignore it.
        if (self._model.shutdown_requested_date is not None
                and (utcnow() - self._model.shutdown_requested_date) < timedelta(seconds=1)):
            logger.debug("Shutdown signal ignored, received twice in less than 1 second")
            return

        logger.warning("Cold shut down")

        # Take down the job execution process with the worker
        if self._model.job_execution_process_pid:
            logger.debug(f"Taking down job execution process {self._model.job_execution_process_pid} with me")
            self.kill_job_execution_process()
            self.wait_for_job_execution_process()
        raise SystemExit()

    def request_stop(self, signum, frame):
        """Stops the current worker loop but waits for child processes to
        end gracefully (warm shutdown).

        Args:
            signum (Any): Signum
            frame (Any): Frame
        """
        logger.debug(f"Got signal {signal_name(signum)}")
        self._model.shutdown_requested_date = utcnow()

        signal.signal(signal.SIGINT, self.request_force_stop)
        signal.signal(signal.SIGTERM, self.request_force_stop)

        self.handle_warm_shutdown_request()
        self._shutdown()

    def _shutdown(self):
        """
        If shutdown is requested in the middle of a job, wait until finish before shutting down and save the request.
        """
        if self._model.state == WorkerStatus.BUSY:
            self._stop_requested = True
            self._model.shutdown_requested_date = utcnow()

            logger.debug(
                "Stopping after current job execution process is finished. Press Ctrl+C again for a cold shutdown.")
            if self.scheduler:
                self.stop_scheduler()
        else:
            if self.scheduler:
                self.stop_scheduler()
            raise StopRequested()

    def handle_warm_shutdown_request(self):
        logger.info(f"Worker {self.name} [PID {self.pid}]: warm shut down requested")

    def reorder_queues(self, reference_queue: Queue):
        """Reorder the queues according to the strategy.
        As this can be defined both in the `Worker` initialization or in the `work` method,
        it doesn't take the strategy directly, but rather uses the private `_dequeue_strategy` attribute.

        :param reference_queue: The queues to reorder
        """
        if self._dequeue_strategy is None:
            self._dequeue_strategy = DequeueStrategy.DEFAULT

        if self._dequeue_strategy not in [e.value for e in DequeueStrategy]:
            raise ValueError(
                f"""Dequeue strategy should be one of {", ".join([e.value for e in DequeueStrategy])}"""
            )
        if self._dequeue_strategy == DequeueStrategy.DEFAULT:
            return
        if self._dequeue_strategy == DequeueStrategy.ROUND_ROBIN:
            pos = self._ordered_queues.index(reference_queue)
            self._ordered_queues = self._ordered_queues[pos + 1:] + self._ordered_queues[: pos + 1]
            return
        if self._dequeue_strategy == DequeueStrategy.RANDOM:
            shuffle(self._ordered_queues)
            return

    def teardown(self):
        if not self._is_job_execution_process:
            if self.scheduler:
                self.stop_scheduler()
            self._model.delete(self.connection)
            self._command_listener.stop()

    def stop_scheduler(self):
        """Stop the scheduler process.
        Will send the kill signal to the scheduler process,
        if there's an OSError, just passes and `join()`'s the scheduler process, waiting for the process to finish.
        """
        if self.scheduler._process and self.scheduler._process.pid:
            try:
                os.kill(self.scheduler._process.pid, signal.SIGTERM)
            except OSError:
                pass
            self.scheduler._process.join()

    def refresh(self):
        """Refreshes the worker data.
        It will get the data from the datastore and update the Worker's attributes
        """
        self._model = WorkerModel.get(self.name, connection=self.connection)
        if self._model is not None:
            self.queues = [
                Queue(name=queue_name, connection=self.connection)
                for queue_name in self._model.queue_names
            ]

    def fork_job_execution_process(self, job: JobModel, queue: Queue) -> None:
        """Spawns a job execution process to perform the actual work and passes it a job.
        This is where the `fork()` actually happens.

        :param job: The job to be executed
        :param queue: The queue from which the job was dequeued
        """
        child_pid = os.fork()
        os.environ["RQ_WORKER_ID"] = self.name
        os.environ["RQ_JOB_ID"] = job.name
        if child_pid == 0:  # Child process/Job executor process to run the job
            os.setsid()
            self.job_executor_process(job, queue)
            os._exit(0)  # just in case
        else:  # Parent worker process
            self._model.set_field("job_execution_process_pid", child_pid, self.connection)
            self.procline("Forked {0} at {1}".format(child_pid, time.time()))

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
        status of the job is set to failed

        :param job: The Job
        :param queue: The Queue
        """
        retpid = ret_val = rusage = None
        job.started_at = utcnow()
        while True:
            try:
                with SCHEDULER_CONFIG.DEATH_PENALTY_CLASS(self.job_monitoring_interval,
                                                          JobExecutionMonitorTimeoutException):
                    retpid, ret_val, rusage = self.wait_for_job_execution_process()
                break
            except JobExecutionMonitorTimeoutException:
                # job execution process has not exited yet and is still running. Send a heartbeat to keep the worker alive.
                self._model.set_current_job_working_time((utcnow() - job.started_at).total_seconds(), self.connection)

                # Kill the job from this side if something is really wrong (interpreter lock/etc).
                if job.timeout != -1 and self.current_job_working_time > (job.timeout + 60):  # type: ignore
                    self._model.heartbeat(self.connection, self.job_monitoring_interval + 60)
                    self.kill_job_execution_process()
                    self.wait_for_job_execution_process()
                    break

                self.maintain_heartbeats(job, queue)

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

        self._model.current_job_working_time = 0
        self._model.job_execution_process_pid = 0
        self._model.save(connection=self.connection)
        if ret_val == os.EX_OK:  # The process exited normally.
            return

        job_status = job.get_status(self.connection)

        if job_status is None:  # Job completed and its ttl has expired
            return
        elif self._model.get_field("stopped_job_name", self.connection) == job.name:
            # job execution process killed deliberately
            logger.warning("Job stopped by user, moving job to FailedJobRegistry")
            if job.stopped_callback:
                job.stopped_callback()
            self.handle_job_failure(job, queue=queue,
                                    exc_string="Job stopped by user, job execution process terminated.")
        elif job_status not in [JobStatus.FINISHED, JobStatus.FAILED]:
            if not job.ended_at:
                job.ended_at = utcnow()

            # Unhandled failure: move the job to the failed queue
            signal_msg = f" (signal {os.WTERMSIG(ret_val)})" if ret_val and os.WIFSIGNALED(ret_val) else ""
            exc_string = f"job-execution-process terminated unexpectedly; waitpid returned {ret_val}{signal_msg}; "
            logger.warning(f"Moving job to FailedJobRegistry ({exc_string})")

            self.handle_job_failure(job, queue=queue, exc_string=exc_string)

    def execute_job(self, job: JobModel, queue: Queue):
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
            self.perform_job(job, queue)
            self._model.set_field("state", WorkerStatus.IDLE, connection=self.connection)

    def maintain_heartbeats(self, job: JobModel, queue: Queue):
        """Updates worker and job's last heartbeat field."""
        with self.connection.pipeline() as pipeline:
            self._model.heartbeat(self.connection, self.job_monitoring_interval + 60)
            ttl = self.get_heartbeat_ttl(job)

            started_job_registry = queue.started_job_registry
            started_job_registry.add(pipeline, current_timestamp() + ttl, self.name, update_existing_only=False)
            results = pipeline.execute()
            if results[2] == 1:
                job.delete(self.connection)

    def job_executor_process(self, job: JobModel, queue: Queue):
        """This is the entry point of the newly spawned job execution process.
        After fork()'ing, assure we are generating random sequences that are different from the worker.

        os._exit() is the way to exit from child processes after a fork(), in contrast to the regular sys.exit()
        """
        random.seed()
        self.setup_job_execution_process_signals()
        self._is_job_execution_process = True
        try:
            self.perform_job(job, queue)
        except:  # noqa
            os._exit(1)
        os._exit(0)

    def setup_job_execution_process_signals(self):
        """Setup signal handing for the newly spawned job execution process

        Always ignore Ctrl+C in the job execution process, as it might abort the currently running job.

        The main worker catches the Ctrl+C and requests graceful shutdown after the current work is done.
        When cold shutdown is requested, it kills the current job anyway.
        """
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def prepare_job_execution(self, job: JobModel):
        """Performs misc bookkeeping like updating states prior to job execution."""
        logger.debug(f"Preparing for execution of Job ID {job.name}")
        with self.connection.pipeline() as pipeline:
            self._model.current_job_name = job.name
            self._model.current_job_working_time = 0

            heartbeat_ttl = self.get_heartbeat_ttl(job)
            self._model.heartbeat(self.connection, heartbeat_ttl)
            queue = get_queue(job.queue_name, connection=self.connection)
            job.prepare_for_execution(self.name, queue.started_job_registry, connection=pipeline)
            pipeline.execute()
            logger.debug("Job preparation finished.")

        self.procline(f"Processing {job.func_name} from {job.queue_name} since {time.time()}")

    def handle_job_success(self, job: JobModel, return_value: Any, queue: Queue):
        """Handles the successful execution of certain job.
        It will remove the job from the `StartedJobRegistry`, adding it to the `SuccessfulJobRegistry`,
        and run a few maintenance tasks including:
            - Resting the current job ID
            - Enqueue dependents
            - Incrementing the job count and working time
            - Handling of the job successful execution

        Runs within a loop with the `watch` method so that protects interactions with dependents keys.

        :param job: The job that was successful.
        :param queue: The queue
        """
        logger.debug(f"Handling successful execution of job {job.name}")
        if job.success_callback is not None:
            success_callback_timeout = job.success_callback_timeout or SCHEDULER_CONFIG.CALLBACK_TIMEOUT
            logger.debug(f"Running success callbacks for {job.name}")
            job.success_callback(job, self.connection, return_value)
        with self.connection.pipeline() as pipeline:
            while True:
                try:
                    if not pipeline.explicit_transaction:
                        # enqueue_dependents didn't call multi after all!
                        # We have to do it ourselves to make sure everything runs in a transaction
                        pipeline.multi()

                    self._model.current_job_name = None
                    self._model.successful_job_count += 1
                    self._model.total_working_time += (job.ended_at - job.started_at).total_seconds()
                    self._model.save(connection=pipeline)
                    if job.result_ttl != 0:
                        logger.debug(f"Saving successful execution result for job {job.name}")
                        queue.job_handle_success(
                            job, result=return_value, result_ttl=job.result_ttl, connection=pipeline)

                    job.expire(job.result_ttl, connection=pipeline)
                    logger.debug(f"Removing job {job.name} from StartedJobRegistry")
                    queue.started_job_registry.delete(pipeline, job.name)

                    pipeline.execute()
                    logger.debug(f"Finished handling successful execution of job {job.name}")
                    break
                except WatchErrorType:
                    continue

    def perform_job(self, job: JobModel, queue: Queue) -> bool:
        """Performs the actual work of a job.
        Called from the process executing the job (forked job execution process).

        :param job: The job to perform
        :param queue: The queue the job was dequeued from
        :returns: True after finished.
        """
        logger.debug("Started Job Registry set.")

        try:
            self.prepare_job_execution(job)

            job.started_at = utcnow()
            timeout = job.timeout or SCHEDULER_CONFIG.DEFAULT_JOB_TIMEOUT
            with SCHEDULER_CONFIG.DEATH_PENALTY_CLASS(timeout, JobTimeoutException, job_id=job.name):
                logger.debug("Performing Job...")
                rv = perform_job(job, self.connection)
                logger.debug(f"Finished performing Job ID {job.name}")

            job.ended_at = utcnow()
            job.last_heartbeat = utcnow()
            job.save(connection=self.connection)
            self.handle_job_success(job=job, return_value=rv, queue=queue)
        except:  # NOQA
            logger.debug(f"Job {job.name} raised an exception.")
            job.ended_at = utcnow()
            exc_info = sys.exc_info()
            exc_string = "".join(traceback.format_exception(*exc_info))

            try:
                job.last_heartbeat = utcnow()
                job.save(connection=self.connection)
                job = JobModel.get(job.name, connection=self.connection)
                if job is not None and job.failure_callback is not None:
                    logger.debug(f"Running failure callbacks for {job.name}")
                    try:
                        job.failure_callback(self, self.connection, traceback.extract_stack())
                    except Exception:  # noqa
                        logger.exception(f"Job {self.name}: error while executing failure callback")
                        raise
            except:  # noqa
                exc_info = sys.exc_info()
                exc_string = "".join(traceback.format_exception(*exc_info))

            self.handle_job_failure(job=job, exc_string=exc_string, queue=queue)
            self.handle_exception(job, *exc_info)
            return False

        logger.info(f"{job.queue_name}: Job OK ({job.name})")
        if rv is not None:
            logger.debug(f"Result: {str(rv)}")

        return True

    def handle_exception(self, job: JobModel, *exc_info):
        """Walks the exception handler stack to delegate exception handling.
        If the job cannot be deserialized, it will raise when func_name or
        the other properties are accessed, which will stop exceptions from
        being properly logged, so we guard against it here.
        """
        logger.debug(f"Handling exception for {job.name}.")
        exc_string = "".join(traceback.format_exception(*exc_info))

        extra = {
            "func": job.func_name,
            "arguments": job.args,
            "kwargs": job.kwargs,
            Queue: job.queue_name,
            "job_name": job.name
        }
        func_name = job.func_name

        # func_name
        logger.error(f"[Job {job.name}]: exception raised while executing ({func_name})\n{exc_string}", extra=extra)

        for handler in self._exc_handlers:
            logger.debug(f"Invoking exception handler {handler}")
            fallthrough = handler(job, *exc_info)

            # Only handlers with explicit return values should disable further
            # exc handling, so interpret a None return value as True.
            if fallthrough is None:
                fallthrough = True

            if not fallthrough:
                break

    def push_exc_handler(self, handler_func):
        """Pushes an exception handler onto the exc handler stack."""
        self._exc_handlers.append(handler_func)

    def pop_exc_handler(self):
        """Pops the latest exception handler off of the exc handler stack."""
        return self._exc_handlers.pop()


class SimpleWorker(Worker):
    def execute_job(self, job: JobModel, queue: Queue):
        """Execute job in same thread/process, do not fork()"""
        self._model.set_field("state", WorkerStatus.BUSY, connection=self.connection)
        self.perform_job(job, queue)
        self._model.set_field("state", WorkerStatus.IDLE, connection=self.connection)

    def get_heartbeat_ttl(self, job: JobModel) -> int:
        """The job timeout + 60 seconds

        :param job: The Job
        :returns: TTL or self.worker_ttl if the job has no timeout (-1)
        """
        if job.timeout == -1:
            return self.worker_ttl
        else:
            return (job.timeout or self.worker_ttl) + 60


class RoundRobinWorker(Worker):
    """Modified version of Worker that dequeues jobs from the queues using a round-robin strategy."""

    def reorder_queues(self, reference_queue):
        pos = self._ordered_queues.index(reference_queue)
        self._ordered_queues = self._ordered_queues[pos + 1:] + self._ordered_queues[: pos + 1]


class RandomWorker(Worker):
    """Modified version of Worker that dequeues jobs from the queues using a random strategy."""

    def reorder_queues(self, reference_queue):
        shuffle(self._ordered_queues)


def _get_ip_address_from_connection(connection: ConnectionType, client_name: str) -> str:
    try:
        connection.client_setname(client_name)
    except ResponseErrorTypes:
        warnings.warn("CLIENT SETNAME command not supported, setting ip_address to unknown", Warning)
        return "unknown"
    client_adresses = [client["addr"] for client in connection.client_list() if client["name"] == client_name]
    if len(client_adresses) > 0:
        return client_adresses[0]
    else:
        warnings.warn("CLIENT LIST command not supported, setting ip_address to unknown", Warning)
        return "unknown"


def _ensure_list(obj: Any) -> List:
    """When passed an iterable of objects, does nothing, otherwise, it returns a list with just that object in it.

    :param obj: The object to ensure is a list
    :return:
    """
    is_nonstring_iterable = isinstance(obj, Iterable) and not isinstance(obj, str)
    return obj if is_nonstring_iterable else [obj]
