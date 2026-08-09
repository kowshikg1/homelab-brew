import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import click

from src.handlers.sqlite import SQLiteHandler
from src.ingestion.ingestion_map import get_handler_class
from src.utils.decorator_utils import ingestion_audit, telegram_alert, timeout
from src.utils.file import load_json
from src.utils.log_util import get_logger
from src.utils.path_variables import PATH_INGESTION_CONFIG

log = get_logger(Path(__file__).stem)


def _apply_overrides(
    config: dict[str, Any], overrides: tuple[str, ...]
) -> dict[str, Any]:
    """Apply key=value overrides for BaseIngestion fields onto a config dict."""
    if not overrides:
        return config

    def _parse_override_value(raw_value: str) -> Any:
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError:
            return raw_value

    allowed_fields = set(BaseIngestion.__dataclass_fields__.keys())
    updated_config = dict(config)

    for override in overrides:
        if '=' not in override:
            raise click.BadParameter(
                f"Invalid override '{override}'. Use key=value format."
            )
        key, raw_value = override.split('=', 1)
        key = key.strip()
        if key not in allowed_fields:
            raise click.BadParameter(
                f"Unsupported override field '{key}'. Allowed fields: {sorted(allowed_fields)}"
            )
        updated_config[key] = _parse_override_value(raw_value.strip())

    return updated_config


class ExtractMode(Enum):
    INCR = 'INCR'
    HIST = 'HIST'


class PublishMode(Enum):
    UPSERT = 'UPSERT'
    APPEND = 'APPEND'
    TRUNCATE = 'TRUNCATE'


@dataclass
class BaseIngestion:
    job_name: str
    handler: str
    extract_method: str
    table: str
    database: str = 'ingestion.db'
    handler_class: str = None
    extract_init: dict[str, Any] = field(default_factory=dict)
    extract_params: dict[str, Any] = field(default_factory=dict)
    extract_mode: ExtractMode = ExtractMode.INCR.value
    publish_mode: PublishMode = PublishMode.UPSERT.value
    last_mtime: str | int = None
    id_config_col: str = None
    watermark_col: str = None
    send_notification: bool = False
    failure_notification: bool = True
    # not much use:
    description: str = ''
    is_active: bool = True
    schedule: str = None
    config_file: str = None
    logging: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.handler_class = (
            get_handler_class(self.handler)
            if not self.handler_class
            else self.handler_class
        )
        self.handler_instance = self.handler_class(**self.extract_init)
        # self.extract_method = self.extract_method or "run"


def insert_data_to_db(job: BaseIngestion, data) -> None:
    sqlite_handler = SQLiteHandler(f'./data/{job.database}')
    if job.publish_mode == PublishMode.TRUNCATE.value:
        sqlite_handler.truncate_table(job.table)

    if job.publish_mode == PublishMode.UPSERT.value:
        sqlite_handler.upsert_data(
            table_name=job.table,
            data=data,
            unique_key=job.id_config_col,
        )
    elif (
        job.extract_mode == ExtractMode.HIST.value
        or job.publish_mode == PublishMode.APPEND.value
    ):
        sqlite_handler.insert_data(
            table_name=job.table,
            data=data,
        )
    else:
        raise ValueError(
            f'Unsupported publish mode: {job.publish_mode} and extract mode: {job.extract_mode} combination.'
        )


@timeout(seconds=600)  # kill the decorated function
@telegram_alert(alert_level='error')
@ingestion_audit(table_name='ingest_audit')
@timeout(seconds=300)
def run(config: dict) -> int:
    job = BaseIngestion(**config)
    extract_function = getattr(job.handler_instance, job.extract_method)
    if not job.last_mtime and job.extract_mode == ExtractMode.INCR.value:
        job.last_mtime = SQLiteHandler(f'./data/{job.database}').get_last_mtime(
            table_name=job.table,
            watermark_col=job.watermark_col,
        )
    if job.last_mtime:
        job.extract_params['last_mtime'] = job.last_mtime
    data = extract_function(**job.extract_params)
    records_count = len(data) if data else 0
    # TODO: add extra meta columns like ingestion time, soft delete flag, etc.
    insert_data_to_db(job, data)
    log.info(
        f"Ingestion job '{job.job_name}' completed successfully. Extracted {records_count} records."
    )
    return records_count


@click.command()
@click.argument('job_name')
@click.option(
    '--override',
    'overrides',
    multiple=True,
    help='Override config using key=value. Can be repeated.',
)
def main(job_name, overrides):
    config = load_json(PATH_INGESTION_CONFIG).get(job_name, None)
    if not config:
        raise ValueError(f"Ingestion job '{job_name}' not found or not active.")
    config = _apply_overrides(config, overrides)
    config['job_name'] = config.get('job_name', job_name)
    run(config)


if __name__ == '__main__':
    main()
