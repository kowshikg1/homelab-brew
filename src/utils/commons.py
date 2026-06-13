import hashlib
import json
from collections import deque
from datetime import UTC, datetime
from typing import Any


def hash_object(obj: Any, encoding: str = 'utf-16') -> str:
    def _make_hash(_obj: Any) -> str:
        if isinstance(_obj, (tuple, list, deque)):
            return str(tuple(_make_hash(i) for i in _obj))
        elif isinstance(_obj, set):
            return str(frozenset(_obj))
        elif isinstance(_obj, dict):
            return str(
                tuple((k, _make_hash(v)) for k, v in sorted(_obj.items()))
            )
        elif callable(_obj):
            return _obj.__name__
        return str(_obj)

    hashed = _make_hash(obj)
    hashed_bytes = bytes(str(hashed), encoding=encoding)
    return hashlib.md5(hashed_bytes).hexdigest()


def to_text(value: Any) -> str:
    if isinstance(value, (dict, list, tuple, set)):  # TODO: Test set.
        return json.dumps(value, default=str)
    elif isinstance(value, bytes):
        return value.decode('utf-8')
    else:
        return str(value)


def current_timestamp(tz=UTC) -> int:
    """Get the current epoch timestamp in seconds."""
    return int(datetime.now(tz).timestamp())
