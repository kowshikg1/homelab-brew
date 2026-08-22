"""Tests for src/monitor/base_monitor.py"""

from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.monitor.base_monitor import MonitorConfig, get_config, run


def _write_yaml(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(content), encoding='utf-8')


class TestGetConfig:
    def test_returns_monitor_config_for_matching_job(self, tmp_path):
        monitor_folder = tmp_path / 'monitor'
        _write_yaml(
            monitor_folder / 'jobs.yml',
            {
                'DELAYED_INGESTION_JOBS': {
                    'handler': 'jobs',
                    'method': 'run_delayed',
                    'params': {'type': 'ingestion'},
                }
            },
        )

        with patch(
            'src.monitor.base_monitor.PATH_MONITOR_FOLDER', monitor_folder
        ):
            config = get_config('DELAYED_INGESTION_JOBS')

        assert isinstance(config, MonitorConfig)
        assert config.handler == 'jobs'
        assert config.method == 'run_delayed'
        assert config.params == {'type': 'ingestion'}

    def test_raises_for_unknown_job_name(self, tmp_path):
        monitor_folder = tmp_path / 'monitor'
        _write_yaml(
            monitor_folder / 'jobs.yml',
            {'KNOWN_JOB': {'handler': 'jobs', 'method': 'run_delayed'}},
        )

        with patch(
            'src.monitor.base_monitor.PATH_MONITOR_FOLDER', monitor_folder
        ):
            with pytest.raises(
                ValueError, match="Monitor job 'MISSING_JOB' not found"
            ):
                get_config('MISSING_JOB')


class TestRun:
    def test_calls_configured_monitor_method_with_params(self):
        monitor_instance = MagicMock()
        monitor_cls = MagicMock(return_value=monitor_instance)

        with (
            patch(
                'src.monitor.base_monitor.get_config',
                return_value=MonitorConfig(
                    handler='jobs',
                    method='run_delayed',
                    params={'type': 'ingestion'},
                ),
            ),
            patch(
                'src.monitor.base_monitor.get_monitor_class',
                return_value=monitor_cls,
            ),
        ):
            run('DELAYED_INGESTION_JOBS')

        monitor_cls.assert_called_once_with()
        monitor_instance.run_delayed.assert_called_once_with(type='ingestion')

    def test_calls_method_with_empty_params_when_none(self):
        monitor_instance = MagicMock()
        monitor_cls = MagicMock(return_value=monitor_instance)

        with (
            patch(
                'src.monitor.base_monitor.get_config',
                return_value=MonitorConfig(
                    handler='jobs',
                    method='run_delayed',
                    params=None,
                ),
            ),
            patch(
                'src.monitor.base_monitor.get_monitor_class',
                return_value=monitor_cls,
            ),
        ):
            run('DELAYED_INGESTION_JOBS')

        monitor_instance.run_delayed.assert_called_once_with()
