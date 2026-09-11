from typing import Any

from redis.exceptions import LockError
from redis.lock import Lock

from scheduler.types import ConnectionType


class KvLock:
    """A lock on a broker key, built on redis-py's `Lock`: only the holder of its token can extend or release it, so a
    holder whose lock expired and was taken over cannot extend or release the new holder's lock."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.acquired = False
        self._lock: Lock | None = None

    @property
    def _locking_key(self) -> str:
        return f"_lock:{self.name}"

    def acquire(self, val: Any, connection: ConnectionType, expire: int | None = None) -> bool:
        """Takes the lock if it is free, without waiting, storing `val` as the owner token.

        `val` must be unique to the taker, and is what `value()` returns while the lock is held.
        """
        # Not thread-local: the scheduler takes its locks in the worker's thread and extends and releases them from its
        # own thread.
        self._lock = Lock(connection, self._locking_key, timeout=expire, thread_local=False)
        self.acquired = self._lock.acquire(blocking=False, token=str(val))
        return self.acquired

    def expire(self, expire: int) -> bool:
        """Resets the lock's TTL to `expire` seconds if this instance still holds it. The lock must have been acquired
        with an expiry.

        :returns: Whether the lock is still held and was extended.
        """
        if self._lock is None:
            return False
        try:
            return self._lock.extend(expire, replace_ttl=True)
        except LockError:  # never acquired, or another holder has the lock now
            return False

    def release(self) -> bool:
        """Releases the lock if this instance still holds it.

        :returns: Whether the lock was held and has been released.
        """
        self.acquired = False
        if self._lock is None:
            return False
        try:
            self._lock.release()
        except LockError:  # never acquired, or another holder has the lock now
            return False
        return True

    def value(self, connection: ConnectionType) -> Any:
        return connection.get(self._locking_key)


class SchedulerLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"lock:scheduler:{queue_name}")


class QueueLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"queue:{queue_name}")
