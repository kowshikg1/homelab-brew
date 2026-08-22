from dataclasses import dataclass

from src.monitor.monitor_map import get_monitor_class
from src.utils.decorator_utils import telegram_alert, timeout
from src.utils.file import load_yaml
from src.utils.log_util import get_logger
from src.utils.path_variables import PATH_MONITOR_FOLDER

log = get_logger(__name__)


@dataclass
class MonitorConfig:
    """Configuration for a monitor."""

    handler: str
    method: str = 'run'
    params: dict | None = None
    # not much use:
    is_active: bool = True
    description: str = ''
    schedule: str = None


def get_config(job_name: str) -> MonitorConfig:
    """Get the monitor configuration for a given job name."""
    for config_file in PATH_MONITOR_FOLDER.rglob('*.yml'):
        configs = load_yaml(config_file)
        if job_name in configs:
            return MonitorConfig(**configs[job_name])
    log.error(
        f"Monitor job '{job_name}' not found in any config file under {PATH_MONITOR_FOLDER}."
    )
    raise ValueError(f"Monitor job '{job_name}' not found.")


@telegram_alert(alert_level='error')
@timeout(seconds=300)
def run(name: str) -> None:
    job = get_config(name)
    monitor_class = get_monitor_class(job.handler)
    getattr(monitor_class(), job.method)(**(job.params or {}))


if __name__ == '__main__':
    import sys

    if len(sys.argv) != 2:
        log.error('Usage: python -m src.monitor.base_monitor <monitor_name>')
        sys.exit(1)
    job_name = sys.argv[1]
    run(job_name)
