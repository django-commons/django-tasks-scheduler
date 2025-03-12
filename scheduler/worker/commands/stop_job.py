import os
from signal import SIGKILL
from typing import Dict, Any

from scheduler.broker_types import ConnectionType
from scheduler.redis_models import WorkerModel
from scheduler.settings import logger
from scheduler.worker.commands.worker_commands import WorkerCommand


class StopJobCommand(WorkerCommand):
    """stop-job command"""

    command_name = "stop-job"

    def __init__(self, *args, job_name: str, worker_name: str, **kwargs) -> None:
        super().__init__(*args, worker_name=worker_name, **kwargs)
        self.job_name = job_name
        if self.job_name is None:
            raise ValueError("job_name is required")

    def command_payload(self) -> Dict[str, Any]:
        return super().command_payload(job_name=self.job_name)

    def process_command(self, connection: ConnectionType) -> None:
        logger.debug(f'Received command to stop job {self.job_name}')
        worker_model = WorkerModel.get(self.worker_name, connection)
        if worker_model is None:
            logger.error(f'Worker {self.worker_name} not found')
            return
        if worker_model.current_job_name == self.job_name:
            os.killpg(os.getpgid(worker_model.job_execution_process_pid), SIGKILL)
            worker_model.set_field("stopped_job_name", self.job_name, connection)
        else:
            logger.info(f"{self.worker_name} not working on job {self.job_name}, command ignored.")
