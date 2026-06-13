"""Tests for src/hooks/run_hook.py"""

from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

from src.hooks.run_hook import CommandResolver, run


class TestCommandResolver:
    def test_returns_run_command_when_present(self):
        @click.command()
        def fake_run():
            return None

        class FakeModule:
            run = fake_run

        resolver = CommandResolver(name='run-hook')
        with patch('src.hooks.run_hook.get_hook', return_value=FakeModule):
            command = resolver.get_command(None, 'forbid-files')

        assert command is fake_run

    def test_raises_value_error_when_hook_has_no_run(self):
        class FakeModule:
            something_else = object()

        resolver = CommandResolver(name='run-hook')
        with patch('src.hooks.run_hook.get_hook', return_value=FakeModule):
            with pytest.raises(
                ValueError, match="does not have a 'run' function"
            ):
                resolver.get_command(None, 'no-run')

    def test_raises_import_error_on_missing_hook(self):
        resolver = CommandResolver(name='run-hook')
        with patch(
            'src.hooks.run_hook.get_hook', side_effect=ImportError('boom')
        ):
            with pytest.raises(ImportError):
                resolver.get_command(None, 'missing')

    def test_raises_value_error_on_invalid_hook_name(self):
        resolver = CommandResolver(name='run-hook')
        with patch(
            'src.hooks.run_hook.get_hook', side_effect=ValueError('bad input')
        ):
            with pytest.raises(ValueError):
                resolver.get_command(None, 'bad')


class TestRunCommand:
    def test_prints_usage_when_no_subcommand(self):
        runner = CliRunner()
        result = runner.invoke(run)

        assert result.exit_code == 0
        assert 'Usage: run-hook HOOK_NAME [ARGS]...' in result.output
        assert 'Use run-hook --help for more information' in result.output

    def test_invokes_dynamic_subcommand(self):
        @click.command()
        def fake_run():
            click.echo('hook executed')

        class FakeModule:
            run = fake_run

        runner = CliRunner()
        with patch('src.hooks.run_hook.get_hook', return_value=FakeModule):
            result = runner.invoke(run, ['forbid-files'])

        assert result.exit_code == 0
        assert 'hook executed' in result.output
        assert 'Usage: run-hook HOOK_NAME [ARGS]...' not in result.output
