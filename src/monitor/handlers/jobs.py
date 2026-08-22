from datetime import datetime

import pandas as pd
from croniter import croniter

from src.handlers.sqlite import SQLiteHandler
from src.handlers.telegram import send_message
from src.utils.commons import current_timestamp
from src.utils.decorator_utils import DEFAULT_TIME_PRECISION
from src.utils.file import load_json
from src.utils.log_util import get_logger
from src.utils.path_variables import INGESTION_SQLITE_DB, PATH_INGESTION_CONFIG

# DEFAULT_TIME_PRECISION = 'ms'
DEFAULT_DELAY_BUFFER_S = 3 * 60

log = get_logger(__name__)


class MonitorJobs:
    def __init__(self):
        pass

    def delayed_ingestion_jobs(self) -> pd.DataFrame:
        """Return ingestion jobs that are delayed beyond a small buffer."""
        start_time = datetime.now()

        jobs = pd.DataFrame(
            [
                {
                    'job': job,
                    'schedule': cfg.get('schedule'),
                }
                for job, cfg in load_json(PATH_INGESTION_CONFIG).items()
            ]
        )

        result_columns = ['job', 'schedule', 'expected_last_run', 'last_run']
        # Skip jobs that do not have a schedule.
        jobs = jobs[
            jobs['schedule'].notna()
            & jobs['schedule'].astype(str).str.strip().ne('')
        ].copy()

        if jobs.empty:
            return pd.DataFrame(columns=result_columns)

        prec_factor = {
            's': 1,
            'ms': 1000,
            'us': 1000000,
        }.get(DEFAULT_TIME_PRECISION, 1)
        jobs['expected_last_run'] = jobs['schedule'].apply(
            lambda schedule: int(
                croniter(schedule, start_time)
                .get_prev(pd.Timestamp)
                .timestamp()
                * prec_factor
            )
        )

        last_run_rows = SQLiteHandler(INGESTION_SQLITE_DB).execute_query(
            'SELECT job_name AS job, MAX(start_time) AS last_run FROM ingest_audit GROUP BY job_name'
        )
        last_run = pd.DataFrame(last_run_rows, columns=['job', 'last_run'])
        if not last_run.empty:
            last_run['last_run'] = pd.to_numeric(
                last_run['last_run'], errors='coerce'
            )

        jobs = jobs.merge(last_run, how='left', on='job')

        now_ms = int(current_timestamp(precision=DEFAULT_TIME_PRECISION))
        due_cutoff = now_ms - DEFAULT_DELAY_BUFFER_S * prec_factor

        delayed_jobs = jobs[
            (
                jobs['expected_last_run'] < due_cutoff
            )  # buffer to avoid false positives due to processing delays
            & (
                jobs['last_run'].isna()
                | (jobs['expected_last_run'] > jobs['last_run'])
            )
        ]

        delayed_jobs = delayed_jobs[result_columns].copy()
        for col in ['expected_last_run', 'last_run']:
            delayed_jobs[col] = pd.to_datetime(
                delayed_jobs[col] // prec_factor, unit='s', errors='coerce'
            )

        return delayed_jobs

    def notify(self, type: str, delayed_jobs: pd.DataFrame) -> None:
        """Notify about delayed jobs."""
        if delayed_jobs.empty:
            log.info(f'No delayed {type} jobs.')
            return

        log.warning(f'{len(delayed_jobs)} delayed {type} jobs detected')
        try:
            message = f'⚠️ Delayed {type} jobs detected:\n<pre>{delayed_jobs.to_markdown(index=False)}</pre>'
            send_message(message, parse_mode='HTML')
            log.info(
                f'Notification sent for {len(delayed_jobs)} delayed {type} jobs'
            )
        except Exception as e:
            log.error(f'Failed to send notification: {e}')

    def run_delayed(self, type: str = 'all') -> None:
        """Run the monitor job based on the specified type."""
        func_map = {
            'ingestion': self.delayed_ingestion_jobs,
        }
        if type not in func_map and type != 'all':
            log.error(
                f"Invalid job type '{type}'. Valid types are: {list(func_map.keys()) + ['all']}"
            )
            raise ValueError(f"Invalid job type '{type}'.")
        for job_type, func in func_map.items():
            if type == job_type or type == 'all':
                delayed_jobs = func()
                self.notify(job_type, delayed_jobs)


if __name__ == '__main__':
    monitor_jobs = MonitorJobs()
    monitor_jobs.notify('ingestion', monitor_jobs.delayed_ingestion_jobs())
