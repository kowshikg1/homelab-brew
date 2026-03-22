import hashlib
import json
import yaml

from collections import deque
from typing import Any

def hash_object(obj: Any, encoding: str = "utf-16") -> str:
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
    if isinstance(value, (dict, list, tuple, set)):#TODO: Test set.
        return json.dumps(value, default=str)
    elif isinstance(value, bytes):
        return value.decode("utf-8")
    else:
        return str(value)

def load_json(file_path: str) -> Any:
    with open(file_path, 'r') as f:
        return json.load(f)

def save_json(data: Any, file_path: str) -> None:
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def load_yaml(file_path: str, encoding='utf-8') -> Any:
    with open(file_path, 'r', encoding=encoding) as f:
        return yaml.safe_load(f)