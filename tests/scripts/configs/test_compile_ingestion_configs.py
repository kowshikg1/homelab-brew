"""Tests for src/scripts/compile_ingestion_configs.py"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.scripts.configs.compile_ingestion_configs import compile_ingestion

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


# ---------------------------------------------------------------------------
# compile_ingestion
# ---------------------------------------------------------------------------


class TestCompileIngestion:
    def test_compiles_single_active_job(self, tmp_path):
        ingestion_dir = tmp_path / 'ingestion'
        _write_yaml(
            ingestion_dir / 'job1.yml',
            'my_job:\n  handler: strava\n  is_active: true\n  extract_method: run\n',
        )
        output_path = tmp_path / 'ingestion_config.json'

        with (
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_FOLDER',
                ingestion_dir,
            ),
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_CONFIG',
                output_path,
            ),
        ):
            compile_ingestion()

        result = json.loads(output_path.read_text())
        assert 'my_job' in result
        assert result['my_job']['handler'] == 'strava'
        actual = Path(result['my_job']['config_file'])
        assert actual.as_posix() in {
            'job1.yml',
            (ingestion_dir / 'job1.yml').as_posix(),
        }

    def test_inactive_job_stored_as_none(self, tmp_path):
        ingestion_dir = tmp_path / 'ingestion'
        _write_yaml(
            ingestion_dir / 'job1.yml',
            'inactive_job:\n  handler: strava\n  is_active: false\n  extract_method: run\n',
        )
        output_path = tmp_path / 'ingestion_config.json'

        with (
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_FOLDER',
                ingestion_dir,
            ),
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_CONFIG',
                output_path,
            ),
        ):
            compile_ingestion()

        result = json.loads(output_path.read_text())
        assert result['inactive_job'] is None

    def test_job_without_is_active_field_treated_as_inactive(self, tmp_path):
        """is_active defaults to falsy (None/not present) → stored as None."""
        ingestion_dir = tmp_path / 'ingestion'
        _write_yaml(
            ingestion_dir / 'job1.yml',
            'no_active_key:\n  handler: strava\n  extract_method: run\n',
        )
        output_path = tmp_path / 'ingestion_config.json'

        with (
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_FOLDER',
                ingestion_dir,
            ),
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_CONFIG',
                output_path,
            ),
        ):
            compile_ingestion()

        result = json.loads(output_path.read_text())
        assert result['no_active_key'] is None

    def test_duplicate_job_name_raises_value_error(self, tmp_path):
        ingestion_dir = tmp_path / 'ingestion'
        _write_yaml(
            ingestion_dir / 'file1.yml',
            'dup_job:\n  handler: strava\n  is_active: true\n  extract_method: run\n',
        )
        _write_yaml(
            ingestion_dir / 'file2.yml',
            'dup_job:\n  handler: youtube\n  is_active: true\n  extract_method: run\n',
        )
        output_path = tmp_path / 'ingestion_config.json'

        with (
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_FOLDER',
                ingestion_dir,
            ),
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_CONFIG',
                output_path,
            ),
        ):
            with pytest.raises(ValueError, match='Duplicate job name'):
                compile_ingestion()

    def test_multiple_jobs_across_files(self, tmp_path):
        ingestion_dir = tmp_path / 'ingestion'
        _write_yaml(
            ingestion_dir / 'a.yml',
            'job_a:\n  handler: strava\n  is_active: true\n  extract_method: run\n',
        )
        _write_yaml(
            ingestion_dir / 'b.yml',
            'job_b:\n  handler: youtube\n  is_active: true\n  extract_method: run\n',
        )
        output_path = tmp_path / 'ingestion_config.json'

        with (
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_FOLDER',
                ingestion_dir,
            ),
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_CONFIG',
                output_path,
            ),
        ):
            compile_ingestion()

        result = json.loads(output_path.read_text())
        assert 'job_a' in result
        assert 'job_b' in result
        actual_a = Path(result['job_a']['config_file'])
        actual_b = Path(result['job_b']['config_file'])
        assert actual_a.as_posix() in {
            'a.yml',
            (ingestion_dir / 'a.yml').as_posix(),
        }
        assert actual_b.as_posix() in {
            'b.yml',
            (ingestion_dir / 'b.yml').as_posix(),
        }

    def test_empty_directory_creates_empty_config(self, tmp_path):
        ingestion_dir = tmp_path / 'ingestion'
        ingestion_dir.mkdir()
        output_path = tmp_path / 'ingestion_config.json'

        with (
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_FOLDER',
                ingestion_dir,
            ),
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_CONFIG',
                output_path,
            ),
        ):
            compile_ingestion()

        result = json.loads(output_path.read_text())
        assert result == {}

    def test_output_is_valid_json(self, tmp_path):
        ingestion_dir = tmp_path / 'ingestion'
        _write_yaml(
            ingestion_dir / 'job1.yml',
            'some_job:\n  handler: strava\n  is_active: true\n  extract_method: run\n',
        )
        output_path = tmp_path / 'ingestion_config.json'

        with (
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_FOLDER',
                ingestion_dir,
            ),
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_CONFIG',
                output_path,
            ),
        ):
            compile_ingestion()

        # Must not raise
        json.loads(output_path.read_text())

    def test_scans_subdirectories_recursively(self, tmp_path):
        ingestion_dir = tmp_path / 'ingestion'
        _write_yaml(
            ingestion_dir / 'subdir' / 'deep_job.yml',
            'deep_job:\n  handler: strava\n  is_active: true\n  extract_method: run\n',
        )
        output_path = tmp_path / 'ingestion_config.json'

        with (
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_FOLDER',
                ingestion_dir,
            ),
            patch(
                'src.scripts.configs.compile_ingestion_configs.PATH_INGESTION_CONFIG',
                output_path,
            ),
        ):
            compile_ingestion()

        result = json.loads(output_path.read_text())
        assert 'deep_job' in result
        actual = Path(result['deep_job']['config_file'])
        assert actual.as_posix() in {
            'subdir/deep_job.yml',
            (ingestion_dir / 'subdir' / 'deep_job.yml').as_posix(),
        }
