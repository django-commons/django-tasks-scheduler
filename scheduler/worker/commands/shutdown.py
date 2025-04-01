import os
import signal

from scheduler.types import ConnectionType
from scheduler.settings import logger
from scheduler.worker.commands.worker_commands import WorkerCommand


class ShutdownCommand(WorkerCommand):
    """shutdown command"""

    command_name = "shutdown"

    def process_command(self, connection: ConnectionType) -> None:
        logger.info("Received shutdown command, sending SIGINT signal.")
        pid = os.getpid()
        os.kill(pid, signal.SIGINT)
