"""Tests for src/utils/file.py"""
import json
import pytest
import tempfile
import os

from src.utils.file import load_json, save_json, load_yaml


# ---------------------------------------------------------------------------
# load_json
# ---------------------------------------------------------------------------

class TestLoadJson:
    def test_load_simple_dict(self, tmp_path):
        data = {"key": "value", "number": 42}
        file = tmp_path / "test.json"
        file.write_text(json.dumps(data))
        result = load_json(str(file))
        assert result == data

    def test_load_list(self, tmp_path):
        data = [1, 2, 3]
        file = tmp_path / "test.json"
        file.write_text(json.dumps(data))
        assert load_json(str(file)) == data

    def test_load_nested_structure(self, tmp_path):
        data = {"outer": {"inner": [1, 2, {"deep": True}]}}
        file = tmp_path / "test.json"
        file.write_text(json.dumps(data))
        assert load_json(str(file)) == data

    def test_load_empty_dict(self, tmp_path):
        file = tmp_path / "test.json"
        file.write_text("{}")
        assert load_json(str(file)) == {}

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_json(str(tmp_path / "missing.json"))

    def test_invalid_json_raises(self, tmp_path):
        file = tmp_path / "bad.json"
        file.write_text("not valid json {{{")
        with pytest.raises(json.JSONDecodeError):
            load_json(str(file))


# ---------------------------------------------------------------------------
# save_json
# ---------------------------------------------------------------------------

class TestSaveJson:
    def test_save_simple_dict(self, tmp_path):
        data = {"a": 1, "b": "two"}
        file = tmp_path / "out.json"
        save_json(data, str(file))
        assert json.loads(file.read_text()) == data

    def test_save_list(self, tmp_path):
        data = [10, 20, 30]
        file = tmp_path / "out.json"
        save_json(data, str(file))
        assert json.loads(file.read_text()) == data

    def test_save_overwrites_existing(self, tmp_path):
        file = tmp_path / "out.json"
        file.write_text('{"old": true}')
        save_json({"new": True}, str(file))
        assert json.loads(file.read_text()) == {"new": True}

    def test_saved_file_is_pretty_printed(self, tmp_path):
        data = {"x": 1}
        file = tmp_path / "out.json"
        save_json(data, str(file))
        content = file.read_text()
        assert "\n" in content  # indent=4 produces newlines

    def test_roundtrip(self, tmp_path):
        data = {"nested": {"list": [1, 2, 3], "flag": False}}
        file = tmp_path / "round.json"
        save_json(data, str(file))
        assert load_json(str(file)) == data


# ---------------------------------------------------------------------------
# load_yaml
# ---------------------------------------------------------------------------

class TestLoadYaml:
    def test_load_simple_mapping(self, tmp_path):
        file = tmp_path / "test.yml"
        file.write_text("key: value\nnumber: 42\n")
        result = load_yaml(str(file))
        assert result == {"key": "value", "number": 42}

    def test_load_list(self, tmp_path):
        file = tmp_path / "test.yml"
        file.write_text("- a\n- b\n- c\n")
        assert load_yaml(str(file)) == ["a", "b", "c"]

    def test_load_nested(self, tmp_path):
        content = "outer:\n  inner:\n    - 1\n    - 2\n"
        file = tmp_path / "test.yml"
        file.write_text(content)
        assert load_yaml(str(file)) == {"outer": {"inner": [1, 2]}}

    def test_load_empty_file(self, tmp_path):
        file = tmp_path / "empty.yml"
        file.write_text("")
        assert load_yaml(str(file)) is None

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_yaml(str(tmp_path / "missing.yml"))

    def test_encoding_parameter(self, tmp_path):
        file = tmp_path / "test.yml"
        file.write_text("name: héllo\n", encoding="utf-8")
        result = load_yaml(str(file), encoding="utf-8")
        assert result == {"name": "héllo"}
