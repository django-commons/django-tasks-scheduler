import errno
import os
import signal
from typing import Optional

from scheduler.broker_types import ConnectionType
from scheduler.redis_models import WorkerModel
from scheduler.settings import logger
from scheduler.worker.commands.worker_commands import WorkerCommand


class KillWorkerCommand(WorkerCommand):
    """kill-worker command"""

    command_name = "kill-worker"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.worker_pid: Optional[int] = None

    def process_command(self, connection: ConnectionType) -> None:
        logger.info("Received kill-worker command.")
        worker_model = WorkerModel.get(self.worker_name, connection)
        self.worker_pid = worker_model.pid
        if self.worker_pid is None:
            raise ValueError("Worker PID is not set")
        logger.info(f"Killing job execution process {self.worker_pid}...")
        try:
            os.killpg(os.getpgid(self.worker_pid), signal.SIGKILL)
            logger.info(f"Killed job execution process pid {self.worker_pid}")
        except OSError as e:
            if e.errno == errno.ESRCH:
                logger.debug("Job execution process already dead")  # "No such process" is fine with us
            else:
                raise
