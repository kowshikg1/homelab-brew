"""Tests for src/handlers/system_stats.py"""
import pytest
from unittest.mock import patch, MagicMock

from src.handlers.system_stats import SystemStats


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def handler():
    return SystemStats(disk_path="/")


@pytest.fixture
def mock_psutil():
    """Patch psutil calls with deterministic values."""
    mem = MagicMock()
    mem.total = 8 * 1024 ** 3   # 8 GB
    mem.used  = 4 * 1024 ** 3   # 4 GB
    mem.percent = 50.0

    swap = MagicMock()
    swap.total = 2 * 1024 ** 3   # 2 GB
    swap.used  = 512 * 1024 ** 2  # 512 MB
    swap.percent = 25.0

    disk = MagicMock()
    disk.total = 500 * 1024 ** 3  # 500 GB
    disk.used  = 200 * 1024 ** 3  # 200 GB
    disk.percent = 40.0

    net = MagicMock()
    net.bytes_sent = 1_000_000
    net.bytes_recv = 5_000_000

    with patch("src.handlers.system_stats.psutil.cpu_percent", return_value=25.0) as mock_cpu, \
         patch("src.handlers.system_stats.psutil.getloadavg", return_value=(1.5, 1.2, 0.9)) as mock_load, \
         patch("src.handlers.system_stats.psutil.virtual_memory", return_value=mem), \
         patch("src.handlers.system_stats.psutil.swap_memory", return_value=swap), \
         patch("src.handlers.system_stats.psutil.disk_usage", return_value=disk) as mock_disk, \
         patch("src.handlers.system_stats.psutil.net_io_counters", return_value=net), \
         patch("src.handlers.system_stats.psutil.boot_time", return_value=1_700_000_000.0), \
         patch("src.handlers.system_stats.psutil.sensors_temperatures", return_value={}):
        yield {"cpu": mock_cpu, "load": mock_load, "disk": mock_disk}


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestSystemStatsInit:
    def test_default_init(self):
        h = SystemStats()
        assert isinstance(h, SystemStats)

    def test_init_accepts_custom_disk_path(self):
        h = SystemStats(disk_path="/mnt/data")
        assert isinstance(h, SystemStats)


# ---------------------------------------------------------------------------
# collect_stats — structure
# ---------------------------------------------------------------------------

class TestCollectStatsStructure:
    def test_returns_list_with_one_record(self, handler, mock_psutil):
        result = handler.collect_stats()
        assert isinstance(result, list)
        assert len(result) == 1

    def test_record_has_all_expected_keys(self, handler, mock_psutil):
        record = handler.collect_stats()[0]
        expected_keys = {
            "collected_at",
            "cpu_percent", "load_avg_1m", "load_avg_5m", "load_avg_15m", "cpu_temp_c",
            "memory_used_mb", "memory_percent",
            "swap_used_mb", "swap_percent",
            "net_bytes_sent", "net_bytes_recv",
            "boot_time",
        }
        assert expected_keys == set(record.keys())


# ---------------------------------------------------------------------------
# collect_stats — values
# ---------------------------------------------------------------------------

class TestCollectStatsValues:
    def test_cpu_percent(self, handler, mock_psutil):
        assert handler.collect_stats()[0]["cpu_percent"] == 25.0

    def test_load_averages(self, handler, mock_psutil):
        record = handler.collect_stats()[0]
        assert record["load_avg_1m"]  == 1.5
        assert record["load_avg_5m"]  == 1.2
        assert record["load_avg_15m"] == 0.9

    def test_memory_values(self, handler, mock_psutil):
        record = handler.collect_stats()[0]
        assert record["memory_used_mb"] == round(4 * 1024, 2)
        assert record["memory_percent"] == 50.0

    def test_swap_values(self, handler, mock_psutil):
        record = handler.collect_stats()[0]
        assert record["swap_used_mb"] == round(512, 2)
        assert record["swap_percent"] == 25.0

    def test_network_values(self, handler, mock_psutil):
        record = handler.collect_stats()[0]
        assert record["net_bytes_sent"] == 1_000_000
        assert record["net_bytes_recv"] == 5_000_000

    def test_boot_time_is_unix_timestamp(self, handler, mock_psutil):
        bt = handler.collect_stats()[0]["boot_time"]
        assert isinstance(bt, int)
        assert bt == 1_700_000_000

    def test_collected_at_is_unix_timestamp(self, handler, mock_psutil):
        ts = handler.collect_stats()[0]["collected_at"]
        assert isinstance(ts, int)

    def test_cpu_percent_uses_interval_1(self, handler, mock_psutil):
        handler.collect_stats()
        mock_psutil["cpu"].assert_called_once_with(interval=1)

    def test_disk_usage_not_called(self, mock_psutil):
        h = SystemStats(disk_path="/mnt/data")
        h.collect_stats()
        mock_psutil["disk"].assert_not_called()


# ---------------------------------------------------------------------------
# _get_cpu_temp
# ---------------------------------------------------------------------------

class TestGetCpuTemp:
    def test_returns_none_when_no_sensors(self, handler):
        with patch("src.handlers.system_stats.psutil.sensors_temperatures", return_value={}):
            assert handler._get_cpu_temp() is None

    def test_returns_none_when_attribute_error(self, handler):
        with patch("src.handlers.system_stats.psutil.sensors_temperatures", side_effect=AttributeError):
            assert handler._get_cpu_temp() is None

    def test_prefers_coretemp(self, handler):
        entry = MagicMock()
        entry.current = 65.0
        with patch("src.handlers.system_stats.psutil.sensors_temperatures",
                   return_value={"coretemp": [entry], "k10temp": [MagicMock(current=70.0)]}):
            assert handler._get_cpu_temp() == 65.0

    def test_falls_back_to_first_sensor(self, handler):
        entry = MagicMock()
        entry.current = 72.5
        with patch("src.handlers.system_stats.psutil.sensors_temperatures",
                   return_value={"acpitz": [entry]}):
            assert handler._get_cpu_temp() == 72.5

    def test_cpu_temp_none_in_record_when_unavailable(self, handler):
        with patch("src.handlers.system_stats.psutil.cpu_percent", return_value=10.0), \
             patch("src.handlers.system_stats.psutil.getloadavg", return_value=(0.1, 0.1, 0.1)), \
             patch("src.handlers.system_stats.psutil.virtual_memory", return_value=MagicMock(total=1, used=1, percent=1.0)), \
             patch("src.handlers.system_stats.psutil.swap_memory", return_value=MagicMock(total=0, used=0, percent=0.0)), \
             patch("src.handlers.system_stats.psutil.disk_usage", return_value=MagicMock(total=1, used=1, percent=1.0)), \
             patch("src.handlers.system_stats.psutil.net_io_counters", return_value=MagicMock(bytes_sent=0, bytes_recv=0)), \
             patch("src.handlers.system_stats.psutil.boot_time", return_value=1_700_000_000.0), \
             patch("src.handlers.system_stats.psutil.sensors_temperatures", return_value={}):
            record = handler.collect_stats()[0]
            assert record["cpu_temp_c"] is None
