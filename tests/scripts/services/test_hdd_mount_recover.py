import pytest

from src.scripts.services.hdd_mount_recover import DiskMapping, GlobalConfig
import src.scripts.services.hdd_mount_recover as module


def test_parse_mapping_valid(tmp_path):
    mapping = tmp_path / "map.yml"
    mapping.write_text(
        """
version: 1
global:
  mount_retry_count: 5
  mount_retry_delay_sec: 2
  docker_restart_timeout_sec: 30
  continue_on_error: false
disks:
  - uuid: "u-1"
    mountpoint: "/mnt/a"
    enabled: true
    containers: ["c1", "c2"]
    images: ["img:1"]
""".strip()
    )

    global_cfg, disks = module._parse_mapping(str(mapping))

    assert isinstance(global_cfg, GlobalConfig)
    assert global_cfg.mount_retry_count == 5
    assert global_cfg.mount_retry_delay_sec == 2
    assert global_cfg.docker_restart_timeout_sec == 30
    assert global_cfg.continue_on_error is False

    assert len(disks) == 1
    assert isinstance(disks[0], DiskMapping)
    assert disks[0].uuid == "u-1"
    assert disks[0].mountpoint == "/mnt/a"
    assert disks[0].containers == ["c1", "c2"]
    assert disks[0].images == ["img:1"]


def test_parse_mapping_requires_disks(tmp_path):
    mapping = tmp_path / "map.yml"
    mapping.write_text("version: 1\nglobal: {}\ndisks: []\n")

    with pytest.raises(ValueError, match="non-empty list"):
        module._parse_mapping(str(mapping))


def test_iter_target_containers_deduplicates(monkeypatch):
    disk = DiskMapping(
        uuid="u-1",
        mountpoint="/mnt/a",
        containers=["c1", "c2"],
        images=["img:1"],
        enabled=True,
    )

    monkeypatch.setattr(module, "_list_running_containers_for_image", lambda _img: ["c2", "c3"])
    result = list(module._iter_target_containers(disk))

    assert result == ["c1", "c2", "c3"]


def test_run_reconciliation_happy_path(monkeypatch, tmp_path):
    mapping = tmp_path / "map.yml"
    mapping.write_text(
        """
version: 1
global:
  mount_retry_count: 1
  mount_retry_delay_sec: 0
  docker_restart_timeout_sec: 15
  continue_on_error: true
disks:
  - uuid: "u-1"
    mountpoint: "/mnt/a"
    containers: ["c1"]
    images: []
""".strip()
    )

    called = {"recover": 0, "restart": 0}

    monkeypatch.setattr(module, "_is_mounted", lambda _mp: True)

    def fake_recover_mount(**_kwargs):
      called["recover"] += 1
      return True

    def fake_restart(_container, _timeout):
      called["restart"] += 1
      return True, ""

    monkeypatch.setattr(module, "_recover_mount", fake_recover_mount)
    monkeypatch.setattr(module, "_restart_container", fake_restart)

    exit_code = module.run_reconciliation(str(mapping), dry_run=False)

    assert exit_code == 0
    assert called["recover"] == 0
    assert called["restart"] == 0


def test_run_reconciliation_mount_failure_continue(monkeypatch, tmp_path):
    mapping = tmp_path / "map.yml"
    mapping.write_text(
        """
version: 1
global:
  mount_retry_count: 1
  mount_retry_delay_sec: 0
  docker_restart_timeout_sec: 15
  continue_on_error: true
disks:
  - uuid: "u-1"
    mountpoint: "/mnt/a"
    containers: ["c1"]
    images: []
""".strip()
    )

    monkeypatch.setattr(module, "_is_mounted", lambda _mp: False)
    monkeypatch.setattr(module, "_recover_mount", lambda **_kwargs: False)

    exit_code = module.run_reconciliation(str(mapping), dry_run=False)
    assert exit_code == 1


def test_run_reconciliation_dry_run_does_not_restart(monkeypatch, tmp_path):
    mapping = tmp_path / "map.yml"
    mapping.write_text(
        """
version: 1
global:
  mount_retry_count: 1
  mount_retry_delay_sec: 0
  docker_restart_timeout_sec: 15
  continue_on_error: true
disks:
  - uuid: "u-1"
    mountpoint: "/mnt/a"
    containers: ["c1"]
    images: []
""".strip()
    )

    called = {"restart": 0}

    monkeypatch.setattr(module, "_is_mounted", lambda _mp: True)

    def fake_restart(_container, _timeout):
        called["restart"] += 1
        return True, ""

    monkeypatch.setattr(module, "_restart_container", fake_restart)

    exit_code = module.run_reconciliation(str(mapping), dry_run=True)
    assert exit_code == 0
    assert called["restart"] == 0
