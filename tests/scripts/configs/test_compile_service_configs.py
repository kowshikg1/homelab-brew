"""Tests for src/scripts/compile_sevice_configs.py"""
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from src.scripts.configs.compile_service_configs import ServiceConfig, load_service_config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MINIMAL_YAML = {
    "test-service": {
        "description": "A test service",
        "exec_module": "src.scripts.test_module",
    }
}

FULL_YAML = {
    "full-service": {
        "description": "Full test service",
        "after": "network.target",
        "wants": "redis.service",
        "type": "simple",
        "user": "testuser",
        "project_path": "projects/myapp",
        "venv_path": "/home/testuser/.venv",
        "exec_module": "src.main",
        "restart": "always",
        "restart_sec": "10",
        "memory_max": "512M",
        "cpu_quota": "50%",
        "environment_pythonpath": "/custom/path",
        "wanted_by": "multi-user.target",
    }
}


@pytest.fixture(autouse=True)
def mock_env_for_service_config(tmp_path):
    """Patch EnvManager at module import level so ServiceConfig defaults don't fail."""
    mock_mgr = MagicMock()
    mock_mgr.get.side_effect = lambda key, default=None: {
        "USER": "testuser",
        "PROJECT_PATH": "projects/myapp",
        "VENV_PATH": "/home/testuser/.venv",
    }.get(key, default)

    with patch("src.scripts.configs.compile_service_configs.EnvManager", return_value=mock_mgr):
        # Re-evaluate module-level `env` binding used in ServiceConfig defaults
        import src.scripts.configs.compile_service_configs as mod
        mod.env = mock_mgr
        yield mock_mgr


# ---------------------------------------------------------------------------
# ServiceConfig
# ---------------------------------------------------------------------------

class TestServiceConfig:
    def test_raises_when_no_exec_module_or_exec_file(self):
        with pytest.raises(ValueError, match="exec_module or exec_file"):
            ServiceConfig(name="svc", description="desc")

    def test_accepts_exec_module(self):
        svc = ServiceConfig(name="svc", description="desc", exec_module="src.main")
        assert svc.exec_module == "src.main"

    def test_accepts_exec_file(self):
        svc = ServiceConfig(name="svc", description="desc", exec_file="/path/to/script.py")
        assert svc.exec_file == "/path/to/script.py"

    def test_default_restart_is_on_failure(self):
        svc = ServiceConfig(name="svc", description="desc", exec_module="src.main")
        assert svc.restart == "on-failure"

    def test_default_restart_sec(self):
        svc = ServiceConfig(name="svc", description="desc", exec_module="src.main")
        assert svc.restart_sec == "5"

    def test_default_wanted_by(self):
        svc = ServiceConfig(name="svc", description="desc", exec_module="src.main")
        assert svc.wanted_by == "multi-user.target"

    def test_default_type_is_simple(self):
        svc = ServiceConfig(name="svc", description="desc", exec_module="src.main")
        assert svc.type == "simple"

    def test_optional_fields_default_to_none(self):
        svc = ServiceConfig(name="svc", description="desc", exec_module="src.main")
        assert svc.after is None
        assert svc.wants is None
        assert svc.memory_max is None
        assert svc.cpu_quota is None
        assert svc.environment_pythonpath is None


# ---------------------------------------------------------------------------
# load_service_config
# ---------------------------------------------------------------------------

class TestLoadServiceConfig:
    def test_returns_string(self, tmp_path):
        yaml_file = tmp_path / "services.yml"
        import yaml
        yaml_file.write_text(yaml.dump(MINIMAL_YAML))

        result = load_service_config(str(yaml_file), "test-service")
        assert isinstance(result, str)

    def test_contains_unit_section(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(MINIMAL_YAML))

        result = load_service_config(str(yaml_file), "test-service")
        assert "[Unit]" in result

    def test_contains_service_section(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(MINIMAL_YAML))

        result = load_service_config(str(yaml_file), "test-service")
        assert "[Service]" in result

    def test_contains_install_section(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(MINIMAL_YAML))

        result = load_service_config(str(yaml_file), "test-service")
        assert "[Install]" in result

    def test_description_in_output(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(MINIMAL_YAML))

        result = load_service_config(str(yaml_file), "test-service")
        assert "A test service" in result

    def test_exec_module_in_output(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(MINIMAL_YAML))

        result = load_service_config(str(yaml_file), "test-service")
        assert "src.scripts.test_module" in result

    def test_after_in_output_when_set(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(FULL_YAML))

        result = load_service_config(str(yaml_file), "full-service")
        assert "network.target" in result

    def test_wants_in_output_when_set(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(FULL_YAML))

        result = load_service_config(str(yaml_file), "full-service")
        assert "redis.service" in result

    def test_memory_max_in_output_when_set(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(FULL_YAML))

        result = load_service_config(str(yaml_file), "full-service")
        assert "512M" in result

    def test_cpu_quota_in_output_when_set(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(FULL_YAML))

        result = load_service_config(str(yaml_file), "full-service")
        assert "50%" in result

    def test_custom_pythonpath_in_output(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(FULL_YAML))

        result = load_service_config(str(yaml_file), "full-service")
        assert "/custom/path" in result

    def test_wanted_by_in_install_section(self, tmp_path):
        import yaml
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(MINIMAL_YAML))

        result = load_service_config(str(yaml_file), "test-service")
        assert "multi-user.target" in result

    def test_exec_file_in_output(self, tmp_path):
        import yaml
        data = {
            "file-service": {
                "description": "File service",
                "exec_file": "/opt/app/run.py",
            }
        }
        yaml_file = tmp_path / "services.yml"
        yaml_file.write_text(yaml.dump(data))

        result = load_service_config(str(yaml_file), "file-service")
        assert "/opt/app/run.py" in result
