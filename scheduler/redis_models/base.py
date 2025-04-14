import dataclasses
import json
from collections.abc import Sequence
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional, Union, Dict, Collection, Any, ClassVar, Set, Type

from redis import Redis

from scheduler.settings import logger
from scheduler.types import ConnectionType, Self

MAX_KEYS = 1000


def as_str(v: Union[bytes, str]) -> Optional[str]:
    """Converts a `bytes` value to a string using `utf-8`.

    :param v: The value (None/bytes/str)
    :raises: ValueError: If the value is not `bytes` or `str`
    :returns: Either the decoded string or None
    """
    if v is None or isinstance(v, str):
        return v
    if isinstance(v, bytes):
        return v.decode("utf-8")
    raise ValueError(f"Unknown type {type(v)} for `{v}`.")


def decode_dict(d: Dict[bytes, bytes], exclude_keys: Set[str]) -> Dict[str, str]:
    return {k.decode(): v.decode() for (k, v) in d.items() if k.decode() not in exclude_keys}


def _serialize(value: Any) -> Optional[Any]:
    if value is None:
        return None
    if isinstance(value, bool):
        value = int(value)
    elif isinstance(value, Enum):
        value = value.value
    elif isinstance(value, datetime):
        value = value.isoformat()
    elif isinstance(value, dict):
        value = json.dumps(value)
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, (list, set, tuple)):
        return json.dumps(value, default=str)
    return str(value)


def _deserialize(value: str, _type: Type) -> Any:
    if value is None:
        return None
    try:
        if _type is str or _type == Optional[str]:
            return as_str(value)
        if _type is datetime or _type == Optional[datetime]:
            return datetime.fromisoformat(as_str(value))
        elif _type is bool:
            return bool(int(value))
        elif _type is int or _type == Optional[int]:
            return int(value)
        elif _type is float or _type == Optional[float]:
            return float(value)
        elif _type in {List[str], Dict[str, str]}:
            return json.loads(value)
        elif _type == Optional[Any]:
            return json.loads(value)
        elif issubclass(_type, Enum):
            return _type(as_str(value))
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to deserialize {value} as {_type}: {e}")
    return value


@dataclasses.dataclass(slots=True, kw_only=True)
class BaseModel:
    name: str
    _element_key_template: ClassVar[str] = ":element:{}"
    # fields that are not serializable using method above and should be dealt with in the subclass
    # e.g. args/kwargs for a job
    _non_serializable_fields: ClassVar[Set[str]] = set()

    @classmethod
    def key_for(cls, name: str) -> str:
        return cls._element_key_template.format(name)

    @property
    def _key(self) -> str:
        return self._element_key_template.format(self.name)

    def serialize(self, with_nones: bool = False) -> Dict[str, str]:
        data = dataclasses.asdict(
            self, dict_factory=lambda fields: {key: value for (key, value) in fields if not key.startswith("_")}
        )
        if not with_nones:
            data = {k: v for k, v in data.items() if v is not None and k not in self._non_serializable_fields}
        for k in data:
            data[k] = _serialize(data[k])
        return data

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> Self:
        types = {f.name: f.type for f in dataclasses.fields(cls) if f.name not in cls._non_serializable_fields}
        for k in data:
            if k not in types:
                logger.warning(f"Unknown field {k} in {cls.__name__}")
                continue
            data[k] = _deserialize(data[k], types[k])
        return cls(**data)


@dataclasses.dataclass(slots=True, kw_only=True)
class HashModel(BaseModel):
    created_at: Optional[datetime] = None
    parent: Optional[str] = None
    _dirty_fields: Set[str] = dataclasses.field(default_factory=set)  # fields that were changed
    _save_all: bool = True  # Save all fields to broker, after init, or after delete
    _list_key: ClassVar[str] = ":list_all:"
    _children_key_template: ClassVar[str] = ":children:{}:"

    def __post_init__(self):
        self._dirty_fields = set()
        self._save_all = True

    def __setattr__(self, key, value):
        if key != "_dirty_fields" and hasattr(self, "_dirty_fields"):
            self._dirty_fields.add(key)
        super(HashModel, self).__setattr__(key, value)

    @property
    def _parent_key(self) -> Optional[str]:
        if self.parent is None:
            return None
        return self._children_key_template.format(self.parent)

    @classmethod
    def all_names(cls, connection: Redis, parent: Optional[str] = None) -> Collection[str]:
        collection_key = cls._children_key_template.format(parent) if parent else cls._list_key
        collection_members = connection.smembers(collection_key)
        return [r.decode() for r in collection_members]

    @classmethod
    def all(cls, connection: Redis, parent: Optional[str] = None) -> List[Self]:
        keys = cls.all_names(connection, parent)
        items = [cls.get(k, connection) for k in keys]
        return [w for w in items if w is not None]

    @classmethod
    def exists(cls, name: str, connection: ConnectionType) -> bool:
        if name is None:
            return False
        return connection.exists(cls._element_key_template.format(name)) > 0

    @classmethod
    def delete_many(cls, names: List[str], connection: ConnectionType) -> None:
        for name in names:
            connection.delete(cls._element_key_template.format(name))

    @classmethod
    def get(cls, name: str, connection: ConnectionType) -> Optional[Self]:
        res = connection.hgetall(cls._element_key_template.format(name))
        if not res:
            return None
        try:
            return cls.deserialize(decode_dict(res, set()))
        except Exception as e:
            logger.warning(f"Failed to deserialize {name}: {e}")
            return None

    @classmethod
    def get_many(cls, names: Sequence[str], connection: ConnectionType) -> List[Self]:
        pipeline = connection.pipeline()
        for name in names:
            pipeline.hgetall(cls._element_key_template.format(name))
        values = pipeline.execute()
        return [(cls.deserialize(decode_dict(v, set())) if v else None) for v in values]

    def save(self, connection: ConnectionType) -> None:
        connection.sadd(self._list_key, self.name)
        if self._parent_key is not None:
            connection.sadd(self._parent_key, self.name)
        mapping = self.serialize(with_nones=True)
        if not self._save_all and len(self._dirty_fields) > 0:
            mapping = {k: v for k, v in mapping.items() if k in self._dirty_fields}
        none_values = {k for k, v in mapping.items() if v is None}
        if none_values:
            connection.hdel(self._key, *none_values)
        mapping = {k: v for k, v in mapping.items() if v is not None}
        if mapping:
            connection.hset(self._key, mapping=mapping)
        self._dirty_fields = set()
        self._save_all = False

    def delete(self, connection: ConnectionType) -> None:
        connection.srem(self._list_key, self._key)
        if self._parent_key is not None:
            connection.srem(self._parent_key, 0, self._key)
        connection.delete(self._key)
        self._save_all = True

    @classmethod
    def count(cls, connection: ConnectionType, parent: Optional[str] = None) -> int:
        if parent is not None:
            result = connection.scard(cls._children_key_template.format(parent))
        else:
            result = connection.scard(cls._list_key)
        return result

    def get_field(self, field: str, connection: ConnectionType) -> Any:
        types = {f.name: f.type for f in dataclasses.fields(self)}
        res = connection.hget(self._key, field)
        return _deserialize(res, types[field])

    def set_field(self, field: str, value: Any, connection: ConnectionType, set_attribute: bool = True) -> None:
        if not hasattr(self, field):
            raise AttributeError(f"Field {field} does not exist")
        if set_attribute:
            setattr(self, field, value)
        if value is None:
            connection.hdel(self._key, field)
            return
        value = _serialize(value)
        connection.hset(self._key, field, value)


@dataclasses.dataclass(slots=True, kw_only=True)
class StreamModel(BaseModel):
    _children_key_template: ClassVar[str] = ":children:{}:"

    def __init__(self, name: str, parent: str, created_at: Optional[datetime] = None):
        self.name = name
        self.created_at: datetime = created_at or datetime.now(timezone.utc)
        self.parent: str = parent

    @property
    def _parent_key(self) -> str:
        return self._children_key_template.format(self.parent)

    @classmethod
    def all(cls, connection: ConnectionType, parent: str) -> List[Self]:
        results = connection.xrevrange(cls._children_key_template.format(parent), "+", "-")
        return [cls.deserialize(decode_dict(result[1], exclude_keys=set())) for result in results]

    def save(self, connection: ConnectionType) -> bool:
        result = connection.xadd(self._parent_key, self.serialize(), maxlen=10)
        return bool(result)
