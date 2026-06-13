"""Tests for src/hooks/custom_hooks/forbid_files.py"""

from unittest.mock import patch

from click.testing import CliRunner

from src.hooks.custom_hooks.forbid_files import forbid_files, run


class TestForbidFilesMatcher:
    def test_blocks_exact_env_file(self):
        assert forbid_files('.env') is True

    def test_blocks_env_variant(self):
        assert forbid_files('.env.prod') is True

    def test_blocks_log_file(self):
        assert forbid_files('service.log') is True

    def test_allows_safe_file(self):
        assert forbid_files('README.md') is False


class TestForbidFilesRunCommand:
    def test_exit_code_zero_when_files_allowed(self):
        runner = CliRunner()
        result = runner.invoke(run, ['README.md'])

        assert result.exit_code == 0

    def test_exit_code_one_when_file_blocked(self):
        runner = CliRunner()
        result = runner.invoke(run, ['README.md', '.env'])

        assert result.exit_code == 1

    def test_delegates_to_run_hook_on_files(self):
        runner = CliRunner()
        with patch(
            'src.hooks.custom_hooks.forbid_files.run_hook_on_files',
            return_value=0,
        ) as mock_run:
            result = runner.invoke(run, ['a.txt', 'b.txt'])

        assert result.exit_code == 0
        assert mock_run.call_count == 1
        hook_func, files = mock_run.call_args[0]
        assert hook_func is forbid_files
        assert tuple(files) == ('a.txt', 'b.txt')
