from typing import Any

from scheduler.redis_models.base import as_str
from scheduler.settings import SCHEDULER_CONFIG
from scheduler.types import Broker, ConnectionType

LUA_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""

LUA_EXPIRE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
else
    return 0
end
"""


class KvLock:
    def __init__(self, name: str) -> None:
        self.name = name
        self.acquired = False
        self.val: Any = None

    @property
    def _locking_key(self) -> str:
        return f"_lock:{self.name}"

    def acquire(self, val: Any, connection: ConnectionType, expire: int | None = None) -> bool:
        self.val = val
        self.acquired = bool(connection.set(self._locking_key, val, nx=True, ex=expire))
        return self.acquired

    def expire(self, connection: ConnectionType, expire: int | None = None, val: Any = None) -> bool:
        check_val = val if val is not None else self.val
        if check_val is not None and expire is not None:
            if SCHEDULER_CONFIG.BROKER == Broker.FAKEREDIS:
                current = connection.get(self._locking_key)
                if current is not None and as_str(current) == str(check_val):
                    return bool(connection.expire(self._locking_key, expire))
                return False
            try:
                res = connection.eval(LUA_EXPIRE_SCRIPT, 1, self._locking_key, str(check_val), expire)
                return bool(res)
            except Exception:
                current = connection.get(self._locking_key)
                if current is not None and as_str(current) == str(check_val):
                    return bool(connection.expire(self._locking_key, expire))
                return False
        return bool(connection.expire(self._locking_key, expire))

    def release(self, connection: ConnectionType, val: Any = None) -> bool:
        check_val = val if val is not None else self.val
        if check_val is not None:
            if SCHEDULER_CONFIG.BROKER == Broker.FAKEREDIS:
                current = connection.get(self._locking_key)
                if current is not None and as_str(current) == str(check_val):
                    connection.delete(self._locking_key)
                    return True
                return False
            try:
                res = connection.eval(LUA_RELEASE_SCRIPT, 1, self._locking_key, str(check_val))
                return bool(res)
            except Exception:
                current = connection.get(self._locking_key)
                if current is not None and as_str(current) == str(check_val):
                    connection.delete(self._locking_key)
                    return True
                return False
        connection.delete(self._locking_key)
        return True

    def value(self, connection: ConnectionType) -> Any:
        return connection.get(self._locking_key)


class SchedulerLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"lock:scheduler:{queue_name}")


class QueueLock(KvLock):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"queue:{queue_name}")
