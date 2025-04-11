from scheduler.types import ConnectionType
from scheduler.redis_models import WorkerModel
from scheduler.settings import logger
from scheduler.worker.commands.worker_commands import WorkerCommand


class SuspendWorkCommand(WorkerCommand):
    """Suspend worker command"""

    command_name = "suspend"

    def process_command(self, connection: ConnectionType) -> None:
        logger.debug(f"Received command to suspend worker {self.worker_name}")
        worker_model = WorkerModel.get(self.worker_name, connection)
        if worker_model is None:
            logger.warning(f"Worker {self.worker_name} not found")
        if worker_model.is_suspended:
            logger.warning(f"Worker {self.worker_name} already suspended")
            return
        worker_model.set_field("is_suspended", True, connection=connection)
        logger.info(f"Worker {self.worker_name} suspended")


class ResumeWorkCommand(WorkerCommand):
    """Resume worker command"""

    command_name = "resume"

    def process_command(self, connection: ConnectionType) -> None:
        logger.debug(f"Received command to resume worker {self.worker_name}")
        worker_model = WorkerModel.get(self.worker_name, connection)
        if worker_model is None:
            logger.warning(f"Worker {self.worker_name} not found")
        if not worker_model.is_suspended:
            logger.warning(f"Worker {self.worker_name} not suspended and therefore can't be resumed")
            return
        worker_model.set_field("is_suspended", False, connection=connection)
        logger.info(f"Worker {self.worker_name} resumed")
