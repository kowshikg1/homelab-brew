"""Tests for src/monitor/monitor_map.py"""

import pytest

from src.monitor.handlers.jobs import MonitorJobs
from src.monitor.monitor_map import MONITOR_MAP, get_monitor_class


class TestMonitorMap:
    def test_map_contains_jobs_handler(self):
        assert 'jobs' in MONITOR_MAP

    def test_jobs_maps_to_monitor_jobs_class(self):
        assert MONITOR_MAP['jobs'] is MonitorJobs


class TestGetMonitorClass:
    def test_returns_jobs_class(self):
        assert get_monitor_class('jobs') is MonitorJobs

    def test_unknown_monitor_raises_value_error(self):
        with pytest.raises(ValueError, match="Monitor 'unknown' not found"):
            get_monitor_class('unknown')
