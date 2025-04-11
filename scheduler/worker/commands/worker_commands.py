import json
from abc import ABC
from datetime import datetime, timezone
from typing import Type, Dict, Any

from scheduler.settings import logger
from scheduler.types import ConnectionType, Self

_PUBSUB_CHANNEL_TEMPLATE: str = ":workers:pubsub:{}"


class WorkerCommandError(Exception):
    pass


class WorkerCommand(ABC):
    """Abstract class for commands to be sent to a worker and processed by worker"""

    _registry: Dict[str, Type[Self]] = dict()
    command_name: str = ""

    def __init__(self, *args, worker_name: str, **kwargs) -> None:
        self.worker_name = worker_name

    def command_payload(self, **kwargs) -> Dict[str, Any]:
        commands_channel = WorkerCommandsChannelListener._commands_channel(self.worker_name)
        payload = {
            "command": self.command_name,
            "worker_name": self.worker_name,
            "channel_name": commands_channel,
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        if kwargs:
            payload.update(kwargs)
        return payload

    def __str__(self) -> str:
        return f"{self.command_name}[{self.command_payload()}]"

    def process_command(self, connection: ConnectionType) -> None:
        raise NotImplementedError

    @classmethod
    def __init_subclass__(cls, *args, **kwargs):
        if cls is WorkerCommand:
            return
        if not cls.command_name:
            raise NotImplementedError(f"{cls.__name__} must have a name attribute")
        WorkerCommand._registry[cls.command_name] = cls

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> Type[Self]:
        command_name = payload.get("command")
        command_class = WorkerCommand._registry.get(command_name)
        if command_class is None:
            raise WorkerCommandError(f"Invalid command: {command_name}")
        return command_class(**payload)


def send_command(connection: ConnectionType, command: WorkerCommand) -> None:
    """Send a command to the worker"""
    payload = command.command_payload()
    connection.publish(payload["channel_name"], json.dumps(payload))


class WorkerCommandsChannelListener(object):
    def __init__(self, connection: ConnectionType, worker_name: str) -> None:
        self.connection = connection
        self.pubsub_channel_name = WorkerCommandsChannelListener._commands_channel(worker_name)

    @staticmethod
    def _commands_channel(worker_name: str) -> str:
        return _PUBSUB_CHANNEL_TEMPLATE.format(worker_name)

    def start(self):
        """Subscribe to this worker's channel"""
        logger.info(f"Subscribing to channel {self.pubsub_channel_name}")
        self.pubsub = self.connection.pubsub()
        self.pubsub.subscribe(**{self.pubsub_channel_name: self.handle_payload})
        self.pubsub_thread = self.pubsub.run_in_thread(sleep_time=0.2, daemon=True)

    def stop(self):
        """Unsubscribe from pubsub channel"""
        if self.pubsub_thread:
            logger.info(f"Unsubscribing from channel {self.pubsub_channel_name}")
            self.pubsub_thread.stop()
            self.pubsub_thread.join()
            self.pubsub.unsubscribe()
            self.pubsub.close()

    def handle_payload(self, payload: str) -> None:
        """Handle commands"""
        command = WorkerCommand.from_payload(json.loads(payload["data"]))
        logger.debug(f"Received command: {command}")
        command.process_command(self.connection)
