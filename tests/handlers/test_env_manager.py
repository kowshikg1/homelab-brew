"""Tests for src/handlers/env_manager.py"""
import os
import pytest
from pathlib import Path
from unittest.mock import patch

from src.handlers.env_manager import EnvManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_env_file(path: Path, content: str) -> None:
    path.write_text(content)


# ---------------------------------------------------------------------------
# Initialisation & load_env
# ---------------------------------------------------------------------------

class TestEnvManagerInit:
    def test_default_env_file_attribute(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("")
        mgr = EnvManager(str(env_file))
        assert mgr.env_file == str(env_file)

    def test_env_vars_is_dict(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("")
        mgr = EnvManager(str(env_file))
        assert isinstance(mgr.env_vars, dict)

    def test_missing_env_file_loads_empty(self, tmp_path):
        missing = tmp_path / "missing.env"
        mgr = EnvManager(str(missing))
        assert mgr.env_vars == {}

    def test_missing_env_file_logs_error(self, tmp_path, caplog):
        missing = tmp_path / "missing.env"
        import logging
        with caplog.at_level(logging.ERROR):
            EnvManager(str(missing))
        assert "not found" in caplog.text.lower() or "no environment" in caplog.text.lower()


class TestLoadEnv:
    def test_parses_key_value_pairs(self, tmp_path):
        f = tmp_path / ".env"
        write_env_file(f, "FOO=bar\nBAZ=qux\n")
        mgr = EnvManager(str(f))
        assert mgr.env_vars == {"FOO": "bar", "BAZ": "qux"}

    def test_strips_whitespace_from_keys_and_values(self, tmp_path):
        f = tmp_path / ".env"
        write_env_file(f, "  KEY  =  value  \n")
        mgr = EnvManager(str(f))
        assert mgr.env_vars["KEY"] == "value"

    def test_ignores_comment_lines(self, tmp_path):
        f = tmp_path / ".env"
        write_env_file(f, "# This is a comment\nKEY=val\n")
        mgr = EnvManager(str(f))
        assert "# This is a comment" not in mgr.env_vars
        assert mgr.env_vars["KEY"] == "val"

    def test_ignores_blank_lines(self, tmp_path):
        f = tmp_path / ".env"
        write_env_file(f, "\nKEY=val\n\n")
        mgr = EnvManager(str(f))
        assert mgr.env_vars == {"KEY": "val"}

    def test_value_with_equals_sign(self, tmp_path):
        """Only the first '=' splits key and value."""
        f = tmp_path / ".env"
        write_env_file(f, "DB_URL=postgresql://user:pass@host/db?opt=val\n")
        mgr = EnvManager(str(f))
        assert mgr.env_vars["DB_URL"] == "postgresql://user:pass@host/db?opt=val"

    def test_empty_value(self, tmp_path):
        f = tmp_path / ".env"
        write_env_file(f, "EMPTY=\n")
        mgr = EnvManager(str(f))
        assert mgr.env_vars["EMPTY"] == ""


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------

class TestEnvManagerGet:
    def setup_method(self, tmp_path):
        pass

    def _mgr(self, tmp_path, content):
        f = tmp_path / ".env"
        write_env_file(f, content)
        return EnvManager(str(f))

    def test_get_existing_key(self, tmp_path):
        mgr = self._mgr(tmp_path, "NAME=Alice\n")
        assert mgr.get("NAME") == "Alice"

    def test_get_missing_key_returns_none_by_default(self, tmp_path):
        mgr = self._mgr(tmp_path, "NAME=Alice\n")
        assert mgr.get("MISSING") is None

    def test_get_missing_key_returns_custom_default(self, tmp_path):
        mgr = self._mgr(tmp_path, "NAME=Alice\n")
        assert mgr.get("MISSING", "default_val") == "default_val"

    def test_get_returns_string(self, tmp_path):
        mgr = self._mgr(tmp_path, "PORT=8080\n")
        assert mgr.get("PORT") == "8080"


# ---------------------------------------------------------------------------
# set & save_env
# ---------------------------------------------------------------------------

class TestEnvManagerSet:
    def _mgr(self, tmp_path, content=""):
        f = tmp_path / ".env"
        write_env_file(f, content)
        return EnvManager(str(f)), f

    def test_set_single_key(self, tmp_path):
        mgr, _ = self._mgr(tmp_path)
        mgr.set("NEW_KEY", "new_val")
        assert mgr.get("NEW_KEY") == "new_val"

    def test_set_overwrites_existing_key(self, tmp_path):
        mgr, _ = self._mgr(tmp_path, "KEY=old\n")
        mgr.set("KEY", "new")
        assert mgr.get("KEY") == "new"

    def test_set_with_kwargs_sets_multiple(self, tmp_path):
        mgr, _ = self._mgr(tmp_path)
        mgr.set("A", "1", B="2", C="3")
        assert mgr.get("A") == "1"
        assert mgr.get("B") == "2"
        assert mgr.get("C") == "3"

    def test_set_persists_to_file(self, tmp_path):
        mgr, f = self._mgr(tmp_path)
        mgr.set("PERSIST", "yes")
        content = f.read_text()
        assert "PERSIST=yes" in content

    def test_kwargs_also_persisted_to_file(self, tmp_path):
        mgr, f = self._mgr(tmp_path)
        mgr.set("A", "1", B="2")
        content = f.read_text()
        assert "A=1" in content
        assert "B=2" in content


# ---------------------------------------------------------------------------
# save_env
# ---------------------------------------------------------------------------

class TestSaveEnv:
    def test_saved_file_can_be_reloaded(self, tmp_path):
        f = tmp_path / ".env"
        write_env_file(f, "KEY=val\n")
        mgr = EnvManager(str(f))
        mgr.set("KEY2", "val2")

        # Reload with a new instance
        mgr2 = EnvManager(str(f))
        assert mgr2.get("KEY") == "val"
        assert mgr2.get("KEY2") == "val2"

    def test_save_writes_all_vars(self, tmp_path):
        f = tmp_path / ".env"
        write_env_file(f, "A=1\nB=2\n")
        mgr = EnvManager(str(f))
        mgr.save_env()
        content = f.read_text()
        assert "A=1" in content
        assert "B=2" in content

    def test_save_format_is_key_equals_value(self, tmp_path):
        f = tmp_path / ".env"
        write_env_file(f, "X=y\n")
        mgr = EnvManager(str(f))
        mgr.save_env()
        for line in f.read_text().splitlines():
            if line.strip():
                assert "=" in line
