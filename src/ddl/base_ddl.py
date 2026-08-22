import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from src.ddl.ddl_utils import (
    DEFAULT_DB,
    DDLMethod,
    enhance_configs_with_hash,
    enhance_configs_with_tag,
    get_new_configs,
)
from src.ddl.methods import get_queries
from src.utils.commons import get_git_head
from src.utils.decorator_utils import telegram_alert, timeout
from src.utils.log_util import get_logger
from src.utils.path_variables import INGESTION_SQLITE_DB, SQLITE_DB_FOLDER

log = get_logger(__name__)

DEFAULT_METHOD_EXECUTION_ORDER = [
    DDLMethod.CREATE,
]


@dataclass
class BaseDDLConfig:
    """Base configuration for DDL operations."""

    method: DDLMethod
    tag: str
    params: dict[str, Any]
    hash: str  # populated by enhance_configs_with_hash
    order: float = float('inf')  # smallest order value executes first

    def __post_init__(self):
        self.method = DDLMethod(self.method)


def run_configs(
    configs: list[BaseDDLConfig],
    original_config_map: dict[str, dict[str, Any]]
    | None = None,  # For logging purposes
):
    configs.sort(
        key=lambda cfg: (
            cfg.order,
            DEFAULT_METHOD_EXECUTION_ORDER.index(cfg.method),
        )
    )
    unique_databases = {
        cfg.params.get('database', DEFAULT_DB) for cfg in configs
    }
    queries = [q for cfg in configs for q in get_queries(cfg)]
    commit_hash = get_git_head()
    try:
        # FIXME: Avoid/fix database cross references.
        with sqlite3.connect(INGESTION_SQLITE_DB) as conn:
            for db in unique_databases:
                db_path = SQLITE_DB_FOLDER / f'{db}.db'
                conn.execute(f"ATTACH DATABASE '{db_path}' AS {db};")
            for query in queries:
                conn.execute(query)
            # push executed ddls to ingest_ddl table
            conn.executemany(
                'INSERT INTO ingest_ddl (id, method, tag, "order", params, commit_hash) VALUES (?, ?, ?, ?, ?, ?)',
                [
                    (
                        cfg.hash,
                        original_config_map[cfg.hash]['method'],
                        original_config_map[cfg.hash].get('tag'),
                        original_config_map[cfg.hash].get('order'),
                        str(original_config_map[cfg.hash].get('params')),
                        commit_hash,
                    )
                    for cfg in configs
                ],
            )
            log.info('Executed %d DDL configurations', len(configs))
            conn.commit()
        return len(configs)
    except sqlite3.Error as e:
        log.error('SQLite error: %s', e)
        raise


@telegram_alert(alert_level='ERROR')
@timeout(seconds=300)
def main():
    configs = get_new_configs()
    log.info('Found %d new DDL configurations to execute', len(configs))

    enhance_configs_with_hash(configs)
    org_cfg_map = {cfg['hash']: cfg.copy() for cfg in configs}
    enhance_configs_with_tag(configs)
    configs = [BaseDDLConfig(**cfg) for cfg in configs]

    configs_by_tag = defaultdict(list)
    for cfg in configs:
        configs_by_tag[cfg.tag].append(cfg)

    # run each tag group
    total_executed = 0
    for _tag, cfgs in configs_by_tag.items():
        # TODO: Don't early exit on error, continue with next tag group and raise all at end
        total_executed += run_configs(cfgs, original_config_map=org_cfg_map)

    log.info('Total executed DDL configurations: %d', total_executed)
    return total_executed


if __name__ == '__main__':
    # don't change the name of this function, as it is used in crontab_tasks/prod_deploy.yml
    main()
