import errno
import os
import signal
from typing import Any

from scheduler.redis_models import WorkerModel
from scheduler.settings import logger
from scheduler.types import ConnectionType
from scheduler.worker.commands.worker_commands import WorkerCommand


class KillWorkerCommand(WorkerCommand):
    """kill-worker command"""

    command_name = "kill-worker"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def process_command(self, connection: ConnectionType) -> None:
        from scheduler.worker import Worker
        logger.info(f"Received kill-worker command for {self.worker_name}")
        worker_model = WorkerModel.get(self.worker_name, connection)
        if worker_model is None or worker_model.pid is None:
            raise ValueError("Worker PID is not set")
        logger.info(f"Killing worker main process {worker_model.pid}...")
        try:
            Worker.from_model(worker_model).request_stop(signal.SIGTERM, None)
            os.killpg(os.getpgid(worker_model.pid), signal.SIGTERM)
            logger.info(f"Killed worker main process pid {worker_model.pid}")
        except OSError as e:
            if e.errno == errno.ESRCH:
                logger.debug(
                    f"Worker main process for {self.worker_name}:{worker_model.pid} already dead"
                )  # "No such process" is fine with us
            else:
                raise
