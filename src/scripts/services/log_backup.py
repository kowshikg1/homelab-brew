# TODO:reduce duplicated code, currently based on db_backup.py for simplicity (also no tests yet)
from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass
from pathlib import Path

from src.utils.decorator_utils import script_execution_audit, telegram_alert
from src.utils.file import load_yaml
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _REPO_ROOT / 'configs' / 'services' / 'log_backup.yml'


@dataclass
class HddConfig:
    enabled: bool
    mountpoint: str
    dest_subdir: str


@dataclass
class CloudConfig:
    enabled: bool
    rclone_remote: str
    dest_path: str


@dataclass
class LogBackupConfig:
    source_dir: Path
    hdd: HddConfig
    cloud: CloudConfig


def _load_config() -> LogBackupConfig:
    raw = load_yaml(_CONFIG_PATH)
    return LogBackupConfig(
        source_dir=_REPO_ROOT / raw['source_dir'],
        hdd=HddConfig(**raw['hdd']),
        cloud=CloudConfig(**raw['cloud']),
    )


def _is_mounted(mountpoint: str) -> bool:
    result = subprocess.run(
        ['findmnt', '-rn', mountpoint], capture_output=True, check=False
    )
    return result.returncode == 0


def _rsync(src: Path, dest: Path) -> None:
    # --append-verify: resumes interrupted transfers; only sends new bytes for growing files
    # not deleting files on the destination (retain old logs)
    dest.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ['rsync', '-a', '--append-verify', f'{src}/', str(dest)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f'rsync failed: {result.stderr.strip()}')


def backup_hdd(config: LogBackupConfig) -> None:
    hdd = config.hdd
    if not hdd.enabled:
        log.info('HDD log backup disabled, skipping')
        return
    if not _is_mounted(hdd.mountpoint):
        raise RuntimeError(
            f'HDD not mounted at {hdd.mountpoint}. Run hdd_mount_recover first.'
        )

    dest = Path(hdd.mountpoint) / hdd.dest_subdir
    _rsync(config.source_dir, dest)
    log.info('HDD log backup complete → %s', dest)


def backup_cloud(config: LogBackupConfig) -> None:
    cloud = config.cloud
    if not cloud.enabled:
        log.info('Cloud log backup disabled, skipping')
        return

    result = subprocess.run(
        ['which', 'rclone'], capture_output=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(
            'rclone not found. Install it and configure a remote.'
        )

    # Use the synced HDD copy as source to avoid reading live log files directly
    hdd_dest = Path(config.hdd.mountpoint) / config.hdd.dest_subdir
    if not hdd_dest.exists():
        raise RuntimeError(
            f'HDD log backup not found at {hdd_dest}. Run HDD backup first.'
        )
    log.info('Cloud log backup source: %s', hdd_dest)

    remote_dest = f'{cloud.rclone_remote}:{cloud.dest_path}'
    # Currently log files are small, so no compression is done
    result = subprocess.run(
        ['rclone', 'sync', str(hdd_dest), remote_dest],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f'rclone failed: {result.stderr.strip()}')
    log.info('Cloud log backup complete → %s', remote_dest)


@telegram_alert()
@script_execution_audit()
def main() -> None:
    parser = argparse.ArgumentParser(
        description='Backup logs/ to HDD and cloud'
    )
    parser.add_argument(
        '--target',
        choices=['hdd', 'cloud', 'all'],
        default='all',
        help='Backup destination (default: all)',
    )
    args = parser.parse_args()

    config = _load_config()

    if args.target in ('hdd', 'all'):
        backup_hdd(config)

    if args.target in ('cloud', 'all'):
        backup_cloud(config)


if __name__ == '__main__':
    main()
