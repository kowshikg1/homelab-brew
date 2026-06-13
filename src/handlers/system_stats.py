from datetime import UTC, datetime
from pathlib import Path

import psutil

from src.utils.decorator_utils import timeout
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)


class SystemStats:
    """Collects stats from the local Linux system."""

    def __init__(self, disk_path: str = '/') -> None:
        # self.disk_path = disk_path # Skipping disk usage collection for now
        pass

    def _get_cpu_temp(self) -> float | None:
        """Return the first available CPU temperature in °C, or None if unavailable."""
        try:
            temps = psutil.sensors_temperatures()
        except AttributeError:
            return None
        if not temps:
            return None
        # Prefer 'coretemp' (Intel) or 'k10temp' (AMD); fall back to first sensor
        for sensor_name in ('coretemp', 'k10temp', 'cpu_thermal'):
            entries = temps.get(sensor_name, [])
            if entries:
                return round(entries[0].current, 1)
        first_entries = next(iter(temps.values()), [])
        return round(first_entries[0].current, 1) if first_entries else None

    @timeout(30)
    def collect_stats(self) -> list[dict]:
        """Collect a single snapshot of system metrics."""

        def _round_mb(bytes_val: int) -> float:
            return round(bytes_val / 1024 / 1024, 2)

        collected_at = int(datetime.now(UTC).timestamp())

        # CPU
        cpu_percent = psutil.cpu_percent(interval=1)
        load_1, load_5, load_15 = psutil.getloadavg()
        cpu_temp_c = self._get_cpu_temp()

        # Memory
        mem = psutil.virtual_memory()
        memory_total_mb = _round_mb(mem.total)
        memory_used_mb = _round_mb(mem.used)
        memory_percent = mem.percent

        # Swap
        swap = psutil.swap_memory()
        # swap_total_mb = _round_mb(swap.total)
        swap_used_mb = _round_mb(swap.used)
        swap_percent = swap.percent

        # Disk
        # skipping disk usage collection

        # Network (cumulative counters since boot)
        net = psutil.net_io_counters()
        net_bytes_sent = net.bytes_sent
        net_bytes_recv = net.bytes_recv

        # Boot time
        boot_time = psutil.boot_time()

        log.info(
            f'Stats collected: cpu={cpu_percent}% load={load_1:.2f}, '
            f'mem={memory_used_mb}/{memory_total_mb} MB ({memory_percent}%), '
            f'temp={cpu_temp_c}°C'
        )

        return [
            {
                'collected_at': collected_at,
                # CPU
                'cpu_percent': cpu_percent,
                'load_avg_1m': round(load_1, 2),
                'load_avg_5m': round(load_5, 2),
                'load_avg_15m': round(load_15, 2),
                'cpu_temp_c': cpu_temp_c,
                # Memory
                'memory_used_mb': memory_used_mb,
                'memory_percent': memory_percent,
                # Swap
                'swap_used_mb': swap_used_mb,
                'swap_percent': swap_percent,
                # Network
                'net_bytes_sent': net_bytes_sent,
                'net_bytes_recv': net_bytes_recv,
                # System
                'boot_time': int(boot_time),
            }
        ]


if __name__ == '__main__':
    handler = SystemStats()
    stats = handler.collect_stats()
    print(stats)
