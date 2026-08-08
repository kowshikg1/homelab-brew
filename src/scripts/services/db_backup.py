from __future__ import annotations

import argparse
import hashlib
import shutil
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from src.utils.decorator_utils import telegram_alert
from src.utils.file import load_yaml
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _REPO_ROOT / 'configs' / 'services' / 'db_backup.yml'
_TIMESTAMP_FMT = '%Y-%m-%d_%H-%M'


@dataclass
class HddConfig:
    enabled: bool
    mountpoint: str
    dest_subdir: str
    retention_count: int


@dataclass
class CloudConfig:
    enabled: bool
    rclone_remote: str
    dest_path: str
    retention_count: int


@dataclass
class BackupConfig:
    source_dir: Path
    backup_name: str
    hdd: HddConfig
    cloud: CloudConfig


def _load_config() -> BackupConfig:
    raw = load_yaml(_CONFIG_PATH)
    return BackupConfig(
        source_dir=_REPO_ROOT / raw['source_dir'],
        backup_name=raw['backup_name'],
        hdd=HddConfig(**raw['hdd']),
        cloud=CloudConfig(**raw['cloud']),
    )


def _timestamp() -> str:
    return datetime.now(UTC).strftime(_TIMESTAMP_FMT)


def _is_mounted(mountpoint: str) -> bool:
    result = subprocess.run(
        ['findmnt', '-rn', mountpoint], capture_output=True, check=False
    )
    return result.returncode == 0


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def _backup_sqlite(src: Path, dest: Path) -> None:
    """Use SQLite online backup API — safe while the DB is being written."""
    with sqlite3.connect(src) as src_conn, sqlite3.connect(dest) as dst_conn:
        src_conn.backup(dst_conn)


def _integrity_check_dir(path: Path) -> None:
    """Run PRAGMA integrity_check on every .db file in path."""
    for db_file in path.glob('*.db'):
        with sqlite3.connect(db_file) as conn:
            result = conn.execute('PRAGMA integrity_check').fetchone()
            if result[0] != 'ok':
                raise RuntimeError(
                    f'Integrity check failed for {db_file}: {result[0]}'
                )
            log.info('Integrity OK: %s', db_file.name)


def _copy_files(src: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for src_file in src.iterdir():
        if not src_file.is_file():
            continue
        dest_file = dest / src_file.name
        if src_file.suffix == '.db':
            _backup_sqlite(src_file, dest_file)
            log.info('SQLite backup: %s → %s', src_file.name, dest_file)
        else:
            # Skip if destination already exists and content is identical
            if dest_file.exists() and _file_sha256(src_file) == _file_sha256(
                dest_file
            ):
                log.debug('Unchanged, skipping: %s', src_file.name)
                continue
            shutil.copy2(src_file, dest_file)
            log.info('Copied: %s → %s', src_file.name, dest_file)


def _prune_old_snapshots(backup_root: Path, retention_count: int) -> None:
    """Delete the oldest snapshot directories beyond retention_count."""
    snapshots = sorted(backup_root.iterdir()) if backup_root.exists() else []
    to_delete = snapshots[: max(0, len(snapshots) - retention_count)]
    for old in to_delete:
        shutil.rmtree(old)
        log.info('Pruned old snapshot: %s', old)


@telegram_alert()
def backup_hdd(config: BackupConfig) -> None:
    hdd = config.hdd
    if not hdd.enabled:
        log.info('HDD backup disabled, skipping')
        return
    if not _is_mounted(hdd.mountpoint):
        raise RuntimeError(
            f'HDD not mounted at {hdd.mountpoint}. Run hdd_mount_recover first.'
        )

    backup_root = Path(hdd.mountpoint) / hdd.dest_subdir
    dest = backup_root / _timestamp()
    _copy_files(config.source_dir, dest)
    _prune_old_snapshots(backup_root, hdd.retention_count)
    log.info('HDD backup complete → %s', dest)


@telegram_alert()
def backup_cloud(config: BackupConfig) -> None:
    cloud = config.cloud
    if not cloud.enabled:
        log.info('Cloud backup disabled, skipping')
        return

    result = subprocess.run(
        ['which', 'rclone'], capture_output=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(
            'rclone not found. Install it and configure a remote.'
        )

    # Use the latest HDD snapshot as source so the live DB is not read directly
    hdd_backup_root = Path(config.hdd.mountpoint) / config.hdd.dest_subdir
    snapshots = (
        sorted(hdd_backup_root.iterdir()) if hdd_backup_root.exists() else []
    )
    if not snapshots:
        raise RuntimeError(
            f'No HDD snapshots found at {hdd_backup_root}. Run HDD backup first.'
        )
    cloud_source = snapshots[-1]  # Use the latest snapshot for cloud backup
    log.info('Cloud backup source: %s', cloud_source)

    _integrity_check_dir(cloud_source)

    timestamp = _timestamp()
    remote_dest = f'{cloud.rclone_remote}:{cloud.dest_path}/{timestamp}'
    result = subprocess.run(
        ['rclone', 'copy', str(cloud_source), remote_dest],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f'rclone failed: {result.stderr.strip()}')
    log.info('Cloud backup complete → %s', remote_dest)

    # Prune old cloud snapshots
    list_result = subprocess.run(
        ['rclone', 'lsd', f'{cloud.rclone_remote}:{cloud.dest_path}'],
        capture_output=True,
        text=True,
        check=False,
    )
    if list_result.returncode == 0:
        snapshots = sorted(
            line.split()[-1]
            for line in list_result.stdout.splitlines()
            if line.strip()
        )
        to_delete = snapshots[: max(0, len(snapshots) - cloud.retention_count)]
        for old in to_delete:
            subprocess.run(
                [
                    'rclone',
                    'purge',
                    f'{cloud.rclone_remote}:{cloud.dest_path}/{old}',
                ],
                check=False,
            )
            log.info('Pruned old cloud snapshot: %s', old)


def main() -> None:
    parser = argparse.ArgumentParser(description='Backup data/ databases')
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
