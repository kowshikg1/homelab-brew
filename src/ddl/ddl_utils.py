from collections.abc import Iterator
from enum import Enum
from pathlib import Path
from typing import Any

from src.handlers.sqlite import SQLiteHandler
from src.utils.commons import (
    current_timestamp,
    hash_object,
    resize_list_iter,
)
from src.utils.file import load_yaml
from src.utils.path_variables import INGESTION_SQLITE_DB, PATH_DDL_FOLDER

DEFAULT_DB = Path(INGESTION_SQLITE_DB).stem


class DDLMethod(Enum):
    """Enumeration of supported DDL methods."""

    CREATE = 'CREATE'


def _get_configs_iter(
    config_dir: Path = PATH_DDL_FOLDER,
) -> Iterator[dict[str, Any]]:
    """Yield YAML configurations for chunking."""
    for cfg_file in sorted(config_dir.rglob('*.yml')):
        yield load_yaml(cfg_file)


def _not_executed_configs(
    configs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return only configs whose hash does not exist in ingest_ddl."""
    if not configs:
        return []

    cfg_by_hash = {hash_object(cfg): cfg for cfg in configs}
    placeholders = ', '.join('?' for _ in cfg_by_hash)
    query = f'SELECT id FROM ingest_ddl WHERE id IN ({placeholders})'

    db_handler = SQLiteHandler(INGESTION_SQLITE_DB)
    executed_hashes = {
        row[0]
        for row in db_handler.execute_query(query, list(cfg_by_hash.keys()))
    }

    return [
        cfg
        for cfg_hash, cfg in cfg_by_hash.items()
        if cfg_hash not in executed_hashes
    ]


def get_new_configs(
    config_dir: Path = PATH_DDL_FOLDER, chunk_size: int = 100
) -> list[dict[str, Any]]:
    """Load YAML configs and return only those not executed yet."""
    if chunk_size <= 0:
        raise ValueError('chunk_size must be a positive integer')

    pending_configs: list[dict[str, Any]] = []
    for config_chunk in resize_list_iter(
        _get_configs_iter(config_dir), chunk_size
    ):
        pending_configs.extend(_not_executed_configs(config_chunk))

    return pending_configs


def enhance_configs_with_hash(
    configs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach a unique hash to each config."""
    for cfg in configs:
        cfg['hash'] = hash_object(cfg)
    return configs


def enhance_configs_with_tag(
    configs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach a shared run tag to each config."""
    run_tag = hash_object(current_timestamp(precision='us'))
    for cfg in configs:
        if 'tag' not in cfg or cfg['tag'] is None:
            cfg['tag'] = run_tag
    return configs
