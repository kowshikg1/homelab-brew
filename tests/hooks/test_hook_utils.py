"""Tests for src/hooks/hook_utils.py"""

from unittest.mock import MagicMock, patch

import pytest

from src.hooks.hook_utils import get_hook, run_hook_on_files


class TestGetHook:
    def test_rejects_empty_name(self):
        with pytest.raises(ValueError, match='non-empty string'):
            get_hook('')

    def test_rejects_non_string_name(self):
        with pytest.raises(ValueError, match='non-empty string'):
            get_hook(None)

    def test_imports_kebab_case_hook_module(self):
        mock_module = MagicMock()
        with patch(
            'src.hooks.hook_utils.importlib.import_module',
            return_value=mock_module,
        ) as mock_import:
            result = get_hook('forbid-files')

        mock_import.assert_called_once_with(
            'src.hooks.custom_hooks.forbid_files'
        )
        assert result is mock_module

    def test_raises_descriptive_import_error(self):
        with patch(
            'src.hooks.hook_utils.importlib.import_module',
            side_effect=ImportError('boom'),
        ):
            with pytest.raises(ImportError) as exc:
                get_hook('missing-hook')

        message = str(exc.value)
        assert "Hook 'missing-hook' not found" in message
        assert 'src.hooks.custom_hooks.missing_hook' in message


class TestRunHookOnFiles:
    def test_returns_zero_when_all_files_pass(self):
        def always_pass(_file_path):
            return False

        assert run_hook_on_files(always_pass, ['a.txt', 'b.txt']) == 0

    def test_returns_one_when_any_file_fails(self):
        def fails_on_env(file_path):
            return file_path == '.env'

        assert run_hook_on_files(fails_on_env, ['ok.txt', '.env']) == 1

    def test_accepts_iterables_not_only_lists(self):
        seen = []

        def recorder(file_path):
            seen.append(file_path)
            return False

        files = (name for name in ['1.txt', '2.txt'])
        assert run_hook_on_files(recorder, files) == 0
        assert seen == ['1.txt', '2.txt']
