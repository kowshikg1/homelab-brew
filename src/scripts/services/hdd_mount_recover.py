from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from src.utils.file import load_yaml
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)


@dataclass
class GlobalConfig:
    mount_retry_count: int = 3
    mount_retry_delay_sec: int = 4
    docker_restart_timeout_sec: int = 20
    continue_on_error: bool = True


@dataclass
class DiskMapping:
    uuid: str
    mountpoint: str
    containers: list[str]
    images: list[str]
    enabled: bool = True


def _run(command: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _is_mounted(mountpoint: str) -> bool:
    result = _run(["findmnt", "-rn", mountpoint])
    return result.returncode == 0


def _recover_mount(mountpoint: str, retries: int, delay_sec: int) -> bool:
    for attempt in range(1, retries + 1):
        direct = _run(["mount", mountpoint])
        if direct.returncode != 0:
            _run(["mount", "-a"])

        if _is_mounted(mountpoint):
            log.info("Recovered mount %s on attempt %s", mountpoint, attempt)
            return True

        log.warning(
            "Mount recovery attempt %s failed for %s; retrying in %ss",
            attempt,
            mountpoint,
            delay_sec,
        )
        time.sleep(delay_sec)

    return False


def _list_running_containers_for_image(image: str) -> list[str]:
    result = _run(["docker", "ps", "--filter", f"ancestor={image}", "--format", "{{.Names}}"])
    if result.returncode != 0:
        err = result.stderr.strip() or "docker ps failed"
        raise RuntimeError(err)

    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _restart_container(container: str, timeout_sec: int) -> tuple[bool, str]:
    result = _run(["docker", "restart", "-t", str(timeout_sec), container])
    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip() or "unknown docker error"
        return False, err
    return True, ""


def _parse_mapping(mapping_file: str) -> tuple[GlobalConfig, list[DiskMapping]]:
    payload = load_yaml(mapping_file)
    if not isinstance(payload, dict):
        raise ValueError("Mapping YAML root must be a dictionary")

    global_raw = payload.get("global") or {}
    disks_raw = payload.get("disks")

    if not isinstance(disks_raw, list) or not disks_raw:
        raise ValueError("'disks' must be a non-empty list")

    global_config = GlobalConfig(
        mount_retry_count=int(global_raw.get("mount_retry_count", 3)),
        mount_retry_delay_sec=int(global_raw.get("mount_retry_delay_sec", 4)),
        docker_restart_timeout_sec=int(global_raw.get("docker_restart_timeout_sec", 20)),
        continue_on_error=bool(global_raw.get("continue_on_error", True)),
    )

    disk_mappings: list[DiskMapping] = []
    for index, disk in enumerate(disks_raw):
        if not isinstance(disk, dict):
            raise ValueError(f"Disk entry at index {index} must be a dictionary")

        uuid = str(disk.get("uuid", "")).strip()
        mountpoint = str(disk.get("mountpoint", "")).strip()
        enabled = bool(disk.get("enabled", True))

        containers = disk.get("containers") or []
        images = disk.get("images") or []

        if not uuid:
            raise ValueError(f"Disk entry at index {index} is missing 'uuid'")
        if not mountpoint:
            raise ValueError(f"Disk entry at index {index} is missing 'mountpoint'")
        if not isinstance(containers, list):
            raise ValueError(f"'containers' must be a list for uuid={uuid}")
        if not isinstance(images, list):
            raise ValueError(f"'images' must be a list for uuid={uuid}")

        disk_mappings.append(
            DiskMapping(
                uuid=uuid,
                mountpoint=mountpoint,
                containers=[str(name).strip() for name in containers if str(name).strip()],
                images=[str(image).strip() for image in images if str(image).strip()],
                enabled=enabled,
            )
        )

    return global_config, disk_mappings


def _iter_target_containers(disk: DiskMapping) -> Iterable[str]:
    seen: set[str] = set()

    for container in disk.containers:
        if container not in seen:
            seen.add(container)
            yield container

    for image in disk.images:
        for container in _list_running_containers_for_image(image):
            if container not in seen:
                seen.add(container)
                yield container


def run_reconciliation(mapping_file: str, dry_run: bool = False) -> int:
    global_config, disks = _parse_mapping(mapping_file)
    any_failure = False

    for disk in disks:
        if not disk.enabled:
            log.info("Skipping disabled disk uuid=%s", disk.uuid)
            continue

        log.info("Processing uuid=%s mountpoint=%s", disk.uuid, disk.mountpoint)

        mounted = _is_mounted(disk.mountpoint)
        if not mounted:
            log.warning("Mount is not healthy for %s", disk.mountpoint)
            if dry_run:
                log.info("Dry-run enabled: skipping mount recovery for %s", disk.mountpoint)
                mounted = False
            else:
                mounted = _recover_mount(
                    mountpoint=disk.mountpoint,
                    retries=global_config.mount_retry_count,
                    delay_sec=global_config.mount_retry_delay_sec,
                )
        else:
            log.info("Mount is healthy for %s", disk.mountpoint)
            continue

        if not mounted:
            any_failure = True
            log.error("Failed to mount uuid=%s at %s", disk.uuid, disk.mountpoint)
            if not global_config.continue_on_error:
                return 1
            continue

        for container in _iter_target_containers(disk):
            if dry_run:
                log.info("Dry-run: would restart container=%s", container)
                continue

            ok, err = _restart_container(container, global_config.docker_restart_timeout_sec)
            if ok:
                log.info("Restarted container=%s", container)
            else:
                any_failure = True
                log.error("Failed to restart container=%s: %s", container, err)
                if not global_config.continue_on_error:
                    return 1

    return 1 if any_failure else 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Ensure UUID-based HDD mounts are available, then restart mapped Docker containers."
        )
    )
    parser.add_argument(
        "--mapping-file",
        default="./configs/services/hdd_mount_recover.yml",
        help="Path to YAML mapping file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without performing mount or container restart operations",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        return run_reconciliation(mapping_file=args.mapping_file, dry_run=args.dry_run)
    except Exception as exc:  # pragma: no cover - top-level guard
        log.exception("Unhandled reconciliation error: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
