import sys

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from src.utils.commons import load_yaml
from src.handlers.sqlite import SQLiteHandler
from src.ingestion.ingestion_map import get_handler_class
from src.utils.decorator_utils import telegram_alert, timeout
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)
INGESTION_CONGIG_PATH = Path("./configs/ingestion_config.json")

class ExtractMode(Enum):
    INCR = "INCR"
    HIST = "HIST"

class PublishMode(Enum):
    UPSERT = "UPSERT"
    APPEND = "APPEND"
    TRUNCATE = "TRUNCATE"

@dataclass
class BaseIngestion:
    handler: str
    extract_method: str
    table: str
    database: str = "ingestion.db"
    handler_class: str = None
    extract_init: dict[str, Any] = field(default_factory=dict)
    extract_params: dict[str, Any] = field(default_factory=dict)
    extract_mode: ExtractMode = ExtractMode.INCR.value
    publish_mode: PublishMode = PublishMode.APPEND.value
    last_mtime: str | int = None
    id_config_col: str = None
    watermark_col: str = None
    send_notification: bool = False
    failure_notification: bool = True
    #not much use:
    description: str = ""
    is_active: bool = True
    schedule: str = None

    def __post_init__(self):
        self.handler_class = get_handler_class(self.handler) if not self.handler_class else self.handler_class
        self.handler_instance = self.handler_class(**self.extract_init)
        # self.extract_method = self.extract_method or "run"


def insert_data_to_db(job: BaseIngestion, data) -> None:
    sqlite_handler = SQLiteHandler(job.database)
    if job.publish_mode == PublishMode.TRUNCATE.value:
        sqlite_handler.truncate_table(job.table)

    if job.publish_mode == PublishMode.UPSERT.value:
        sqlite_handler.upsert_data(
            table_name=job.table,
            data=data,
            unique_key=job.id_config_col,
        )
    elif job.extract_mode == ExtractMode.HIST.value or job.publish_mode == PublishMode.APPEND.value:
        sqlite_handler.insert_data(
            table_name=job.table,
            data=data,
        )
    else:
        raise ValueError(f"Unsupported publish mode: {job.publish_mode} and extract mode: {job.extract_mode} combination.")

@telegram_alert(alert_level="error")
@timeout(seconds=300)
def run(job_name):
    config = load_yaml(INGESTION_CONGIG_PATH).get(job_name, None)
    if not config:
        raise ValueError(f"Ingestion job '{job_name}' not found or not active.")

    job = BaseIngestion(**config)
    extract_function = getattr(job.handler_instance, job.extract_method)
    if not job.last_mtime and job.extract_mode == ExtractMode.INCR.value:
        job.last_mtime = SQLiteHandler(job.database).get_last_mtime(
            table_name=job.table,
            watermark_col=job.watermark_col,
        )
    if job.last_mtime:
        job.extract_params['last_mtime'] = job.last_mtime
    data = extract_function(**job.extract_params)
    insert_data_to_db(job, data)
    log.info(f"Ingestion job '{job_name}' completed successfully. Extracted {len(data)} records.")


if __name__ == "__main__":
    job_name = sys.argv[1]
    run(job_name)