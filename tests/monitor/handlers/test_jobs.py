"""Tests for src/monitor/handlers/jobs.py"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.monitor.handlers.jobs import MonitorJobs


class _FakeCronIter:
    def __init__(self, schedule, _start_time):
        self.schedule = schedule

    def get_prev(self, _timestamp_type):
        values = {
            '*/5 * * * *': pd.Timestamp.fromtimestamp(700),
            '0 * * * *': pd.Timestamp.fromtimestamp(900),
        }
        return values[self.schedule]


class TestDelayedIngestionJobs:
    def test_returns_expected_columns_for_empty_schedule_set(self):
        monitor = MonitorJobs()
        with patch(
            'src.monitor.handlers.jobs.load_json',
            return_value={
                'job_a': {'schedule': ''},
                'job_b': {'schedule': None},
            },
        ):
            result = monitor.delayed_ingestion_jobs()

        assert list(result.columns) == [
            'job',
            'schedule',
            'expected_last_run',
            'last_run',
        ]
        assert result.empty

    def test_returns_only_delayed_jobs(self):
        monitor = MonitorJobs()
        sqlite_mock = MagicMock()
        sqlite_mock.execute_query.return_value = [
            ('job_delayed', 650000),
            ('job_ok', 950000),
        ]

        with (
            patch(
                'src.monitor.handlers.jobs.load_json',
                return_value={
                    'job_delayed': {'schedule': '*/5 * * * *'},
                    'job_ok': {'schedule': '0 * * * *'},
                    'job_no_schedule': {'schedule': ''},
                },
            ),
            patch(
                'src.monitor.handlers.jobs.SQLiteHandler',
                return_value=sqlite_mock,
            ),
            patch(
                'src.monitor.handlers.jobs.croniter', side_effect=_FakeCronIter
            ),
            patch(
                'src.monitor.handlers.jobs.current_timestamp',
                return_value=1000000,
            ),
        ):
            result = monitor.delayed_ingestion_jobs()

        assert len(result) == 1
        assert result.iloc[0]['job'] == 'job_delayed'
        assert pd.notna(result.iloc[0]['expected_last_run'])


class TestNotify:
    def test_no_notification_when_dataframe_empty(self, caplog):
        monitor = MonitorJobs()
        empty = pd.DataFrame(columns=['job', 'schedule'])

        with patch('src.monitor.handlers.jobs.send_message') as mock_send:
            monitor.notify('ingestion', empty)

        mock_send.assert_not_called()
        assert 'No delayed ingestion jobs.' in caplog.text

    def test_sends_html_notification_for_delayed_jobs(self):
        monitor = MonitorJobs()
        delayed = pd.DataFrame(
            [
                {
                    'job': 'job_delayed',
                    'schedule': '*/5 * * * *',
                    'expected_last_run': pd.Timestamp('2026-01-01 00:00:00'),
                    'last_run': pd.Timestamp('2025-12-31 23:40:00'),
                }
            ]
        )

        with patch('src.monitor.handlers.jobs.send_message') as mock_send:
            monitor.notify('ingestion', delayed)

        mock_send.assert_called_once()
        assert 'Delayed ingestion jobs detected' in mock_send.call_args[0][0]
        assert mock_send.call_args[1]['parse_mode'] == 'HTML'


class TestRunDelayed:
    def test_invalid_type_raises_value_error(self):
        monitor = MonitorJobs()
        with pytest.raises(ValueError, match="Invalid job type 'bad_type'"):
            monitor.run_delayed('bad_type')

    def test_runs_single_requested_type(self):
        monitor = MonitorJobs()
        delayed_df = pd.DataFrame([{'job': 'x'}])

        with (
            patch.object(
                monitor,
                'delayed_ingestion_jobs',
                return_value=delayed_df,
            ) as mock_delayed,
            patch.object(monitor, 'notify') as mock_notify,
        ):
            monitor.run_delayed('ingestion')

        mock_delayed.assert_called_once_with()
        mock_notify.assert_called_once_with('ingestion', delayed_df)

    def test_runs_all_types_when_all_requested(self):
        monitor = MonitorJobs()
        delayed_df = pd.DataFrame([{'job': 'x'}])

        with (
            patch.object(
                monitor,
                'delayed_ingestion_jobs',
                return_value=delayed_df,
            ) as mock_delayed,
            patch.object(monitor, 'notify') as mock_notify,
        ):
            monitor.run_delayed('all')

        mock_delayed.assert_called_once_with()
        mock_notify.assert_called_once_with('ingestion', delayed_df)
