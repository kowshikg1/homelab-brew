from dataclasses import dataclass
from pathlib import Path

from src.ddl.ddl_utils import DEFAULT_DB
from src.utils.path_variables import INGESTION_SQLITE_DB


@dataclass
class CreateConfig:
    """Configuration for the CREATE DDL operation."""

    database: str = DEFAULT_DB
    table_name: str | None = None
    columns: list[dict[str, str]] | None = None
    view_name: str | None = None
    view_columns: list[str] | None = None

    def __post_init__(self):
        if not self.table_name and not self.view_name:
            raise ValueError('Either table_name or view_name must be provided.')


def get_table_query(cfg: CreateConfig) -> list[str]:
    """Generate SQL query for creating a table based on the provided configuration."""
    if not cfg.table_name or not cfg.columns:
        return []
    columns_definitions = ', '.join(f'{col}' for col in cfg.columns)
    return [
        f'CREATE TABLE IF NOT EXISTS {cfg.database}.{cfg.table_name} ({columns_definitions});'
    ]


def get_view_query(cfg: CreateConfig) -> list[str]:
    """Generate SQL query for creating a view` based on the provided configuration."""
    if cfg.view_name:
        cfg.table_name = cfg.table_name or cfg.view_name.remove_prefix('vw_')
        cfg.view_columns = cfg.view_columns or ['*']
        columns_definitions = ', '.join(cfg.view_columns)
        return [
            f'CREATE VIEW IF NOT EXISTS {cfg.database}.{cfg.view_name} AS SELECT {columns_definitions} FROM {cfg.database}.{cfg.table_name};'
        ]
    elif cfg.database == Path(INGESTION_SQLITE_DB).stem:
        if not cfg.table_name:
            raise ValueError(
                'table_name must be provided for view creation in the ingestion database.'
            )
        cfg.view_name = f'vw_{cfg.table_name}'
        return [
            f'CREATE VIEW IF NOT EXISTS {cfg.database}.{cfg.view_name} AS SELECT * FROM {cfg.database}.{cfg.table_name};'
        ]
    return []


def run(cfg: dict):
    create_config = CreateConfig(**cfg)
    queries = list()
    queries.extend(get_table_query(create_config))
    queries.extend(get_view_query(create_config))
    return queries


if __name__ == '__main__':
    # Example usage
    example_config = {
        'database': 'ingestion',
        'table_name': 'my_table',
        'columns': ['id INT PRIMARY KEY', 'name TEXT'],
        # "view_name": "vw_my_table",
        # "view_columns": ["id", "name"]
    }
    queries = run(example_config)
    for query in queries:
        print(query)
