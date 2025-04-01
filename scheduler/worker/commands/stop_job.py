import os
import signal
from typing import Dict, Any

from scheduler.types import ConnectionType
from scheduler.redis_models import WorkerModel, JobModel
from scheduler.settings import logger
from scheduler.worker.commands.worker_commands import WorkerCommand, WorkerCommandError


class StopJobCommand(WorkerCommand):
    """stop-job command"""

    command_name = "stop-job"

    def __init__(self, *args, job_name: str, worker_name: str, **kwargs) -> None:
        super().__init__(*args, worker_name=worker_name, **kwargs)
        self.job_name = job_name
        if self.job_name is None:
            raise WorkerCommandError("job_name for kill-job command is required")

    def command_payload(self) -> Dict[str, Any]:
        return super().command_payload(job_name=self.job_name)

    def process_command(self, connection: ConnectionType) -> None:
        logger.debug(f"Received command to stop job {self.job_name}")
        worker_model = WorkerModel.get(self.worker_name, connection)
        job_model = JobModel.get(self.job_name, connection)
        if worker_model is None:
            logger.error(f"Worker {self.worker_name} not found")
            return
        if job_model is None:
            logger.error(f"Job {self.job_name} not found")
            return
        if worker_model.pid == worker_model.job_execution_process_pid:
            logger.warning(f"Job execution process ID and worker process id {worker_model.pid} are equal, skipping")
            return
        if not worker_model.job_execution_process_pid:
            logger.error(f"Worker {self.worker_name} has no job execution process")
            return
        if worker_model.current_job_name != self.job_name:
            logger.info(
                f"{self.worker_name} working on job {worker_model.current_job_name}, "
                f"not on {self.job_name}, kill-job command ignored."
            )
            return
        worker_model.set_field("stopped_job_name", self.job_name, connection)
        try:
            pgid = os.getpgid(worker_model.job_execution_process_pid)
            logger.debug(
                f"worker_pid {worker_model.pid}, job_execution_process {worker_model.job_execution_process_pid}"
            )
            if pgid == worker_model.pid:
                logger.error("No separate process for job execution, skipping")
                return
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError as e:
            logger.error(f"Error killing job {self.job_name}: {e}")
