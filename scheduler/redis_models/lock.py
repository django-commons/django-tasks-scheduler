from typing import Any

from scheduler.types import ConnectionType

# Both scripts act only while the lock still holds the caller's token: a holder whose lock expired and was taken over
# by another process must neither release nor extend the new holder's lock.
_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""

_EXPIRE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""


class KvLock:
    def __init__(self, name: str) -> None:
        self.name = name
        self.acquired = False
        self.val: str | None = None

    @property
    def _locking_key(self) -> str:
        return f"_lock:{self.name}"

    def acquire(self, val: Any, connection: ConnectionType, expire: int | None = None) -> bool:
        """Take the lock if it is free, storing `val` as the owner token.

        Only a holder presenting the same token can later extend or release the lock, so `val` must be unique to the
        process taking it.
        """
        self.val = str(val)
        self.acquired = bool(connection.set(self._locking_key, self.val, nx=True, ex=expire))
        return self.acquired

    def expire(self, connection: ConnectionType, expire: int, val: Any = None) -> bool:
        """Extend the lock's TTL if it is still held with `val` (defaults to the token it was acquired with).

        :returns: Whether the lock is still held and was extended.
        """
        token = str(val) if val is not None else self.val
        if token is None:
            return False
        return bool(connection.eval(_EXPIRE_SCRIPT, 1, self._locking_key, token, expire))

    def release(self, connection: ConnectionType, val: Any = None) -> bool:
        """Release the lock if it is still held with `val` (defaults to the token it was acquired with).

        :returns: Whether the lock was held and has been released.
        """
        token = str(val) if val is not None else self.val
        if token is None:
            return False
        self.acquired = False
        return bool(connection.eval(_RELEASE_SCRIPT, 1, self._locking_key, token))

    def value(self, connection: ConnectionType) -> Any:
        return connection.get(self._locking_key)


class SchedulerLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"lock:scheduler:{queue_name}")


class QueueLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"queue:{queue_name}")
