"""Tests for src/scripts/configs/compile_crontab_jobs.py"""
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import yaml

from src.scripts.configs.compile_crontab_jobs import (
    _slug,
    _is_frequent_schedule,
    _with_logging,
    _with_lock,
    _build_ingestion_command,
    _build_general_command,
    _iter_yaml_files,
    compile_crontab_jobs,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_yaml(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(content, f)


def _write_json(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(content, f)


@pytest.fixture
def mock_env():
    """Mock EnvManager to provide default values."""
    with patch("src.scripts.configs.compile_crontab_jobs.env") as mock:
        mock.get.side_effect = lambda key, default=None: {
            "PROJECT_ROOT": "/home/user/projects/homelab",
            "LOG_FOLDER": "./logs",
        }.get(key, default)
        yield mock


@pytest.fixture
def mock_defaults(tmp_path, mock_env):
    """Patch module-level defaults to use tmp_path."""
    with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_PROJECT_ROOT", tmp_path):
        with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_CRONTAB_TASKS_FOLDER", tmp_path / "crontab"):
            with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_OUTPUT_FILE", tmp_path / "compiled.crontab"):
                with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_LOG_FOLDER", tmp_path / "logs"):
                    yield tmp_path


# ---------------------------------------------------------------------------
# _slug
# ---------------------------------------------------------------------------

class TestSlug:
    def test_lowercase_conversion(self):
        assert _slug("MyJobName") == "myjobname"

    def test_replaces_spaces_with_dash(self):
        assert _slug("my job name") == "my-job-name"

    def test_replaces_multiple_special_chars(self):
        assert _slug("my_job@name!") == "my-job-name"

    def test_strips_leading_trailing_dashes(self):
        assert _slug("---my-job---") == "my-job"

    def test_empty_string(self):
        assert _slug("") == ""

    def test_only_special_chars(self):
        assert _slug("@#$%") == ""


# ---------------------------------------------------------------------------
# _is_frequent_schedule
# ---------------------------------------------------------------------------

class TestIsFrequentSchedule:
    def test_minute_wildcard_is_frequent(self):
        assert _is_frequent_schedule("* * * * *") is True

    def test_every_5_minutes_is_frequent(self):
        assert _is_frequent_schedule("*/5 * * * *") is True

    def test_every_15_minutes_is_frequent(self):
        assert _is_frequent_schedule("*/15 * * * *") is True

    def test_every_30_minutes_not_frequent(self):
        assert _is_frequent_schedule("*/30 * * * *") is False

    def test_hourly_not_frequent(self):
        assert _is_frequent_schedule("0 * * * *") is False

    def test_daily_not_frequent(self):
        assert _is_frequent_schedule("0 0 * * *") is False

    def test_comma_separated_minutes_is_frequent(self):
        assert _is_frequent_schedule("5,10,15 * * * *") is True

    def test_minute_range_is_frequent(self):
        assert _is_frequent_schedule("0-30 * * * *") is True

    def test_invalid_schedule_format(self):
        assert _is_frequent_schedule("invalid") is False

    def test_too_few_fields(self):
        assert _is_frequent_schedule("* * *") is False


# ---------------------------------------------------------------------------
# _with_logging
# ---------------------------------------------------------------------------

class TestWithLogging:
    def test_no_logging_when_append_log_false(self):
        command = "python script.py"
        job_config = {"append_log": False}
        result = _with_logging(command, "test_job", job_config)
        assert result == command

    def test_append_to_log_file_when_set(self):
        command = "python script.py"
        job_config = {"log_file": "test", "append_log": True}
        with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_LOG_FOLDER", Path("./logs")):
            result = _with_logging(command, "test_job", job_config)
            assert ">> " in result
            assert "logs/test-cron.log 2>&1" in result
            assert result.startswith(command)

    def test_log_file_with_explicit_extension(self):
        command = "python script.py"
        job_config = {"log_file": "output.log", "append_log": True}
        with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_LOG_FOLDER", Path("./logs")):
            result = _with_logging(command, "test_job", job_config)
            assert ">> " in result
            assert "logs/output.log 2>&1" in result

    def test_append_log_defaults_to_true_when_log_file_set(self):
        command = "python script.py"
        job_config = {"log_file": "test"}
        with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_LOG_FOLDER", Path("./logs")):
            result = _with_logging(command, "test_job", job_config)
            assert ">> " in result
            assert "logs/test-cron.log 2>&1" in result

    def test_handles_log_file_with_spaces(self):
        command = "python script.py"
        job_config = {"log_file": "my output", "append_log": True}
        with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_LOG_FOLDER", Path("./logs")):
            result = _with_logging(command, "test_job", job_config)
            assert "logs/my-output-cron.log" in result


# ---------------------------------------------------------------------------
# _with_lock
# ---------------------------------------------------------------------------

class TestWithLock:
    def test_no_lock_when_use_lock_false(self):
        command = "python script.py"
        job_config = {"use_lock": False, "schedule": "*/5 * * * *"}
        result = _with_lock(command, "test_job", job_config)
        assert result == command

    def test_adds_lock_for_frequent_schedule(self):
        command = "python script.py"
        job_config = {"schedule": "*/5 * * * *"}
        result = _with_lock(command, "test_job", job_config)
        assert "/usr/bin/flock -n" in result
        assert "/tmp/test-job.lock" in result

    def test_uses_custom_lock_file(self):
        command = "python script.py"
        job_config = {"use_lock": True, "lock_file": "/custom/lock.lock"}
        result = _with_lock(command, "test_job", job_config)
        assert "/custom/lock.lock" in result

    def test_lock_file_generated_from_job_name(self):
        command = "python script.py"
        job_config = {"use_lock": True}
        result = _with_lock(command, "my_ingestion_job", job_config)
        assert "/tmp/my-ingestion-job.lock" in result

    def test_lock_file_quoted_when_contains_spaces(self):
        command = "python script.py"
        job_config = {"use_lock": True, "lock_file": "/tmp/my lock.lock"}
        result = _with_lock(command, "test_job", job_config)
        assert "'/tmp/my lock.lock'" in result

    def test_no_lock_for_infrequent_schedule(self):
        command = "python script.py"
        job_config = {"schedule": "0 2 * * *"}
        result = _with_lock(command, "test_job", job_config)
        assert result == command


# ---------------------------------------------------------------------------
# _build_ingestion_command
# ---------------------------------------------------------------------------

class TestBuildIngestionCommand:
    def test_basic_ingestion_command(self):
        job_config = {"schedule": "0 2 * * *"}
        result = _build_ingestion_command("STRAVA_ACTIVITIES", job_config)
        assert ".venv/bin/python -m src.ingestion.base_ingestion STRAVA_ACTIVITIES" in result

    def test_ingestion_with_custom_python_bin(self):
        job_config = {"schedule": "0 2 * * *", "python_bin": "/usr/bin/python3"}
        result = _build_ingestion_command("YOUTUBE", job_config)
        assert "/usr/bin/python3 -m src.ingestion.base_ingestion YOUTUBE" in result

    def test_ingestion_with_lock_for_frequent_schedule(self):
        job_config = {"schedule": "*/15 * * * *"}
        result = _build_ingestion_command("SYSTEM_STATS", job_config)
        assert "/usr/bin/flock -n" in result
        assert "/tmp/system-stats.lock" in result

    def test_ingestion_with_logging(self):
        job_config = {"schedule": "0 2 * * *", "log_file": "strava", "append_log": True}
        with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_LOG_FOLDER", Path("./logs")):
            result = _build_ingestion_command("RAW_STRAVA_STREAMS", job_config)
            assert ">> " in result
            assert "logs/strava-cron.log 2>&1" in result

    def test_ingestion_with_both_lock_and_logging(self):
        job_config = {"schedule": "*/5 * * * *", "log_file": "docker", "append_log": True}
        with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_LOG_FOLDER", Path("./logs")):
            result = _build_ingestion_command("DOCKER_STATS", job_config)
            assert "/usr/bin/flock -n" in result
            assert "/tmp/docker-stats.lock" in result
            assert ">> " in result
            assert "logs/docker-cron.log 2>&1" in result


# ---------------------------------------------------------------------------
# _build_general_command
# ---------------------------------------------------------------------------

class TestBuildGeneralCommand:
    def test_command_single_string(self):
        job_config = {"command": "echo 'hello world'"}
        result = _build_general_command("echo_job", job_config)
        assert "echo 'hello world'" in result

    def test_command_with_shell_operators(self):
        job_config = {"command": "(cd /tmp && ls) || echo 'failed'"}
        result = _build_general_command("complex_job", job_config)
        assert "(cd /tmp && ls) || echo 'failed'" in result

    def test_commands_list_with_and_operator(self):
        job_config = {"commands": ["echo start", "python script.py", "echo done"], "operator": "&&"}
        result = _build_general_command("multi_cmd", job_config)
        assert "echo start && python script.py && echo done" in result

    def test_commands_list_with_pipe_operator(self):
        job_config = {"commands": ["cat file.txt", "grep pattern"], "operator": "|"}
        result = _build_general_command("pipe_job", job_config)
        assert "cat file.txt | grep pattern" in result

    def test_commands_list_with_semicolon_operator(self):
        job_config = {"commands": ["cmd1", "cmd2", "cmd3"], "operator": ";"}
        result = _build_general_command("seq_job", job_config)
        assert "cmd1 ; cmd2 ; cmd3" in result

    def test_script_python_auto_detected(self):
        job_config = {"script": "src/scripts/my_script.py"}
        result = _build_general_command("script_job", job_config)
        assert ".venv/bin/python" in result
        assert "src/scripts/my_script.py" in result

    def test_script_non_python(self):
        job_config = {"script": "/usr/local/bin/my_script", "python": False}
        result = _build_general_command("bash_job", job_config)
        assert "/usr/local/bin/my_script" in result
        assert ".venv/bin/python" not in result

    def test_script_with_args(self):
        job_config = {"script": "src/scripts/backup.py", "args": ["--dir=/data", "--compress"]}
        result = _build_general_command("backup_job", job_config)
        assert "src/scripts/backup.py" in result
        assert "--dir=/data" in result
        assert "--compress" in result

    def test_wrap_parentheses(self):
        job_config = {"command": "echo test", "wrap_parentheses": True}
        result = _build_general_command("paren_job", job_config)
        assert "( echo test )" in result

    def test_wrap_braces(self):
        job_config = {"command": "echo test", "wrap_braces": True}
        result = _build_general_command("brace_job", job_config)
        assert "{ echo test; }" in result

    def test_extra_command_appended(self):
        job_config = {
            "command": "python main.py",
            "extra_command": "echo 'done' >> status.log",
        }
        result = _build_general_command("extra_job", job_config)
        assert "python main.py echo 'done' >> status.log" in result

    def test_missing_command_raises_error(self):
        job_config = {}
        with pytest.raises(ValueError, match="requires one of"):
            _build_general_command("no_cmd_job", job_config)

    def test_command_with_lock(self):
        job_config = {"command": "python script.py", "use_lock": True}
        result = _build_general_command("locked_job", job_config)
        assert "/usr/bin/flock -n" in result
        assert "/tmp/locked-job.lock" in result

    def test_command_with_logging(self):
        job_config = {"command": "python script.py", "log_file": "mylog", "append_log": True}
        with patch("src.scripts.configs.compile_crontab_jobs.DEFAULT_LOG_FOLDER", Path("./logs")):
            result = _build_general_command("logged_job", job_config)
            assert ">> " in result
            assert "logs/mylog-cron.log 2>&1" in result


# ---------------------------------------------------------------------------
# _iter_yaml_files
# ---------------------------------------------------------------------------

class TestIterYamlFiles:
    def test_finds_yaml_files_in_folder(self, tmp_path):
        folder = tmp_path / "configs"
        _write_yaml(folder / "job1.yml", {"job1": {}})
        _write_yaml(folder / "job2.yml", {"job2": {}})

        files = _iter_yaml_files(folder)
        assert len(files) == 2
        assert any("job1.yml" in str(f) for f in files)
        assert any("job2.yml" in str(f) for f in files)

    def test_ignores_non_yaml_files(self, tmp_path):
        folder = tmp_path / "configs"
        _write_yaml(folder / "job.yml", {"job": {}})
        (folder / "readme.txt").write_text("not yaml")

        files = _iter_yaml_files(folder)
        assert len(files) == 1

    def test_returns_sorted_list(self, tmp_path):
        folder = tmp_path / "configs"
        _write_yaml(folder / "z_job.yml", {})
        _write_yaml(folder / "a_job.yml", {})
        _write_yaml(folder / "m_job.yml", {})

        files = _iter_yaml_files(folder)
        names = [f.stem for f in files]
        assert names == ["a_job", "m_job", "z_job"]

    def test_empty_folder_returns_empty_list(self, tmp_path):
        folder = tmp_path / "empty"
        folder.mkdir()
        files = _iter_yaml_files(folder)
        assert files == []

    def test_nonexistent_folder_returns_empty_list(self, tmp_path):
        folder = tmp_path / "nonexistent"
        files = _iter_yaml_files(folder)
        assert files == []


# ---------------------------------------------------------------------------
# compile_crontab_jobs
# ---------------------------------------------------------------------------

class TestCompileCrontabJobs:
    def test_compiles_ingestion_jobs(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {
                "RAW_STRAVA": {
                    "is_active": True,
                    "handler": "strava",
                    "schedule": "10 2 * * *",
                },
                "SYSTEM_STATS": {
                    "is_active": True,
                    "handler": "system_stats",
                    "schedule": "*/15 * * * *",
                },
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 2
        assert any("RAW_STRAVA" in line for line in lines)
        assert any("SYSTEM_STATS" in line for line in lines)

    def test_skips_base_prefixed_jobs(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {
                "base_strava": {"is_active": True, "handler": "strava", "schedule": "0 0 * * *"},
                "RAW_STRAVA": {"is_active": True, "handler": "strava", "schedule": "0 0 * * *"},
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert any("RAW_STRAVA" in line for line in lines)
        assert not any("base_strava" in line for line in lines)

    def test_skips_inactive_jobs(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {
                "ACTIVE_JOB": {"is_active": True, "handler": "test", "schedule": "0 0 * * *"},
                "INACTIVE_JOB": {"is_active": False, "handler": "test", "schedule": "0 0 * * *"},
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert any("ACTIVE_JOB" in line for line in lines)

    def test_skips_jobs_without_schedule(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {
                "SCHEDULED_JOB": {"is_active": True, "handler": "test", "schedule": "0 0 * * *"},
                "NO_SCHEDULE_JOB": {"is_active": True, "handler": "test"},
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert any("SCHEDULED_JOB" in line for line in lines)

    def test_compiles_general_jobs_from_yaml(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(ingestion_config, {})

        crontab_folder = tmp_path / "crontab"
        _write_yaml(
            crontab_folder / "tasks.yml",
            {
                "backup_job": {
                    "is_active": True,
                    "schedule": "0 1 * * *",
                    "command": "rsync -av /data /backup",
                }
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=crontab_folder,
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert any("rsync -av /data /backup" in line for line in lines)

    def test_skips_example_yml(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(ingestion_config, {})

        crontab_folder = tmp_path / "crontab"
        _write_yaml(
            crontab_folder / "example.yml",
            {
                "example_job": {
                    "is_active": True,
                    "schedule": "0 0 * * *",
                    "command": "echo example",
                }
            },
        )
        _write_yaml(
            crontab_folder / "real_job.yml",
            {
                "real_job": {
                    "is_active": True,
                    "schedule": "0 1 * * *",
                    "command": "echo real",
                }
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=crontab_folder,
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert any("echo real" in line for line in lines)
        assert not any("echo example" in line for line in lines)

    def test_includes_project_root_in_cron_line(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {"TEST_JOB": {"is_active": True, "handler": "test", "schedule": "0 0 * * *"}},
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert "cd" in lines[0]
        assert str(tmp_path) in lines[0]

    def test_uses_custom_repo_for_general_job(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(ingestion_config, {})

        crontab_folder = tmp_path / "crontab"
        _write_yaml(
            crontab_folder / "tasks.yml",
            {
                "custom_repo_job": {
                    "is_active": True,
                    "schedule": "0 1 * * *",
                    "command": "python script.py",
                    "repo": "/custom/repo",
                }
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=crontab_folder,
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert "cd /custom/repo" in lines[0]

    def test_writes_output_to_file(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {"TEST_JOB": {"is_active": True, "handler": "test", "schedule": "0 0 * * *"}},
        )
        output_file = tmp_path / "my_crontab.crontab"

        compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=output_file,
            project_root=tmp_path,
        )

        assert output_file.exists()
        content = output_file.read_text()
        assert "TEST_JOB" in content

    def test_empty_configs_produces_empty_output(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(ingestion_config, {})

        output_file = tmp_path / "empty.crontab"
        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=output_file,
            project_root=tmp_path,
        )

        assert lines == []
        assert output_file.exists()
        assert output_file.read_text() == ""

    def test_logging_config_from_nested_structure(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {
                "LOGGED_JOB": {
                    "is_active": True,
                    "handler": "strava",
                    "schedule": "0 2 * * *",
                    "logging": {"log_file": "strava.log", "append_log": True},
                }
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert ">> " in lines[0]
        assert "strava.log" in lines[0]

    def test_multiple_jobs_multiple_files(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {
                "INGESTION_1": {"is_active": True, "handler": "test1", "schedule": "0 0 * * *"},
                "INGESTION_2": {"is_active": True, "handler": "test2", "schedule": "0 1 * * *"},
            },
        )

        crontab_folder = tmp_path / "crontab"
        _write_yaml(
            crontab_folder / "tasks1.yml",
            {
                "GENERAL_1": {
                    "is_active": True,
                    "schedule": "0 2 * * *",
                    "command": "cmd1",
                }
            },
        )
        _write_yaml(
            crontab_folder / "tasks2.yml",
            {
                "GENERAL_2": {
                    "is_active": True,
                    "schedule": "0 3 * * *",
                    "command": "cmd2",
                }
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=crontab_folder,
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 4
        assert any("INGESTION_1" in line for line in lines)
        assert any("INGESTION_2" in line for line in lines)
        assert any("cmd1" in line for line in lines)
        assert any("cmd2" in line for line in lines)

    def test_frequent_ingestion_job_uses_lock_by_default(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {
                "FREQUENT_JOB": {
                    "is_active": True,
                    "handler": "stats",
                    "schedule": "*/5 * * * *",
                }
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert any("/usr/bin/flock" in line for line in lines)

    def test_infrequent_ingestion_job_no_lock_by_default(self, tmp_path, mock_defaults):
        ingestion_config = tmp_path / "ingestion_config.json"
        _write_json(
            ingestion_config,
            {
                "INFREQUENT_JOB": {
                    "is_active": True,
                    "handler": "test",
                    "schedule": "0 2 * * *",
                }
            },
        )

        lines = compile_crontab_jobs(
            compiled_ingestion_file=ingestion_config,
            general_jobs_folder=tmp_path / "crontab",
            output_file=tmp_path / "compiled.crontab",
            project_root=tmp_path,
        )

        assert len(lines) == 1
        assert "/usr/bin/flock" not in lines[0]
