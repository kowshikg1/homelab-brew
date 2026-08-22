from src.monitor.handlers.jobs import MonitorJobs

MONITOR_MAP = {
    'jobs': MonitorJobs,
}


def get_monitor_class(monitor_name: str):
    """Get the monitor class based on the monitor name."""
    if monitor_name not in MONITOR_MAP:
        raise ValueError(f"Monitor '{monitor_name}' not found in MONITOR_MAP.")
    return MONITOR_MAP[monitor_name]
