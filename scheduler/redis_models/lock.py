from typing import Optional, Any

from scheduler.types import ConnectionType


class KvLock(object):
    def __init__(self, name: str) -> None:
        self.name = name
        self.acquired = False

    @property
    def _locking_key(self) -> str:
        return f"_lock:{self.name}"

    def acquire(self, val: Any, connection: ConnectionType, expire: Optional[int] = None) -> bool:
        self.acquired = connection.set(self._locking_key, val, nx=True, ex=expire)
        return self.acquired

    def expire(self, connection: ConnectionType, expire: Optional[int] = None) -> bool:
        return connection.expire(self._locking_key, expire)

    def release(self, connection: ConnectionType):
        connection.delete(self._locking_key)

    def value(self, connection: ConnectionType) -> Any:
        return connection.get(self._locking_key)


class SchedulerLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"lock:scheduler:{queue_name}")


class QueueLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"queue:{queue_name}")
