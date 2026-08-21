import hashlib
import json
import subprocess
from collections import deque
from collections.abc import Generator
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


def current_timestamp(tz=UTC, precision: str = 's') -> int:
    """Get the current epoch timestamp with configurable precision.

    precision:
        - 's'  : seconds (default)
        - 'ms' : milliseconds
        - 'us' : microseconds
    """
    ts = datetime.now(tz).timestamp()
    if precision == 's':
        return int(ts)
    if precision == 'ms':
        return int(ts * 1000)
    if precision == 'us':
        return int(ts * 1_000_000)
    raise ValueError("precision must be one of: 's', 'ms', 'us'")


def get_git_head(short: bool = False) -> str | None:
    """Return current HEAD commit hash for traceability."""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD']
            if short
            else ['git', 'rev-parse', 'HEAD'],
            check=True,
            capture_output=True,
            text=True,
        )
        commit_hash = result.stdout.strip()
        return commit_hash or None
    except Exception:
        return None


def resize_list_iter(
    lst: Generator[list[Any], None, None], size: int
) -> Generator[list[Any], None, None]:
    """Yield successive `size`-sized chunks from `lst`."""
    if size <= 0:
        raise ValueError('size must be a positive integer')

    excess = []

    for sublist in lst:
        if excess:
            excess.extend(sublist)
            sublist = excess
            excess = []
        i = 0
        n = len(sublist)
        while i + size <= n:
            yield sublist[i : i + size]
            i += size

        if i < n:
            excess = sublist[i:]

    if excess:
        yield excess
