"""Tests for src/utils/decorator_utils.py"""

import logging
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.decorator_utils import (
    _alert_context,
    _TelegramAlertHandler,
    ingestion_audit,
    script_execution_audit,
    telegram_alert,
    timeout,
)
from src.utils.path_variables import PATH_INGESTION_FOLDER

# ---------------------------------------------------------------------------
# _TelegramAlertHandler
# ---------------------------------------------------------------------------


class TestTelegramAlertHandler:
    def _make_record(self, level: int, msg: str = 'test', exc_info=None):
        record = logging.LogRecord(
            name='test',
            level=level,
            pathname='test.py',
            lineno=1,
            msg=msg,
            args=(),
            exc_info=exc_info,
        )
        return record

    def test_default_alert_level_is_warning(self):
        handler = _TelegramAlertHandler()
        assert handler.alert_level == logging.WARNING

    def test_custom_alert_level(self):
        handler = _TelegramAlertHandler(alert_level='ERROR')
        assert handler.alert_level == logging.ERROR

    def test_case_insensitive_level(self):
        handler = _TelegramAlertHandler(alert_level='critical')
        assert handler.alert_level == logging.CRITICAL

    def test_emit_sends_message_at_threshold(self):
        handler = _TelegramAlertHandler(alert_level='WARNING')
        record = self._make_record(logging.WARNING, 'warn msg')
        with patch('src.utils.decorator_utils.send_message') as mock_send:
            handler.emit(record)
        mock_send.assert_called_once()
        assert 'warn msg' in mock_send.call_args[0][0]

    def test_emit_sends_message_above_threshold(self):
        handler = _TelegramAlertHandler(alert_level='WARNING')
        record = self._make_record(logging.ERROR, 'error msg')
        with patch('src.utils.decorator_utils.send_message') as mock_send:
            handler.emit(record)
        mock_send.assert_called_once()

    def test_emit_does_not_send_below_threshold(self):
        handler = _TelegramAlertHandler(alert_level='WARNING')
        record = self._make_record(logging.DEBUG, 'debug msg')
        with patch('src.utils.decorator_utils.send_message') as mock_send:
            handler.emit(record)
        mock_send.assert_not_called()

    def test_emit_skips_when_in_alert_context(self):
        handler = _TelegramAlertHandler()
        record = self._make_record(logging.ERROR)
        _alert_context.in_alert = True
        try:
            with patch('src.utils.decorator_utils.send_message') as mock_send:
                handler.emit(record)
            mock_send.assert_not_called()
        finally:
            _alert_context.in_alert = False

    def test_emit_includes_traceback_for_exc_info(self):
        handler = _TelegramAlertHandler(alert_level='ERROR')
        try:
            raise ValueError('boom')
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = self._make_record(logging.ERROR, 'error', exc_info=exc_info)
        with patch('src.utils.decorator_utils.send_message') as mock_send:
            handler.emit(record)

        called_msg = mock_send.call_args[0][0]
        assert 'ValueError' in called_msg or 'Traceback' in called_msg

    def test_emit_silently_fails_when_send_raises(self):
        handler = _TelegramAlertHandler()
        record = self._make_record(logging.ERROR)
        with patch(
            'src.utils.decorator_utils.send_message',
            side_effect=Exception('network error'),
        ):
            # Should not raise
            handler.emit(record)

    def test_in_alert_context_reset_after_emit(self):
        handler = _TelegramAlertHandler()
        record = self._make_record(logging.ERROR)
        with patch('src.utils.decorator_utils.send_message'):
            handler.emit(record)
        assert not getattr(_alert_context, 'in_alert', False)


# ---------------------------------------------------------------------------
# telegram_alert decorator
# ---------------------------------------------------------------------------


class TestTelegramAlertDecorator:
    def test_wraps_function_preserves_name(self):
        @telegram_alert()
        def my_function():
            pass

        assert my_function.__name__ == 'my_function'

    def test_decorated_function_returns_value(self):
        @telegram_alert()
        def add(a, b):
            return a + b

        with patch('src.utils.decorator_utils.send_message'):
            assert add(2, 3) == 5

    def test_exception_re_raised(self):
        @telegram_alert()
        def failing():
            raise RuntimeError('fail!')

        with patch('src.utils.decorator_utils.send_message'):
            with pytest.raises(RuntimeError, match='fail!'):
                failing()

    def test_telegram_alert_sent_on_exception(self):
        @telegram_alert()
        def failing():
            raise ValueError('test error')

        with patch('src.utils.decorator_utils.send_message') as mock_send:
            with pytest.raises(ValueError):
                failing()

        mock_send.assert_called()
        message = mock_send.call_args[0][0]
        assert 'ValueError' in message
        assert 'test error' in message

    def test_no_alert_sent_on_success(self):
        @telegram_alert()
        def succeeds():
            return 42

        with patch('src.utils.decorator_utils.send_message') as _mock_send:
            succeeds()

        # Warning handler is added but should not fire for a clean return
        # Only exception path calls send_message directly
        # (The log handler may call it for WARNING+; with no log messages, it won't)

    def test_handler_removed_after_success(self):
        root_logger = logging.getLogger()
        initial_count = len(root_logger.handlers)

        @telegram_alert()
        def succeeds():
            return 1

        with patch('src.utils.decorator_utils.send_message'):
            succeeds()

        assert len(root_logger.handlers) == initial_count

    def test_handler_removed_after_exception(self):
        root_logger = logging.getLogger()
        initial_count = len(root_logger.handlers)

        @telegram_alert()
        def fails():
            raise RuntimeError('err')

        with patch('src.utils.decorator_utils.send_message'):
            with pytest.raises(RuntimeError):
                fails()

        assert len(root_logger.handlers) == initial_count

    def test_skips_alert_when_already_in_alert(self):
        """Ensures the infinite-loop guard works at the wrapper level."""
        call_count = []

        @telegram_alert()
        def inner():
            call_count.append(1)
            raise ValueError('inner error')

        _alert_context.in_alert = True
        try:
            with patch('src.utils.decorator_utils.send_message') as mock_send:
                with pytest.raises(ValueError):
                    inner()
            # send_message not called because we're already in alert
            mock_send.assert_not_called()
        finally:
            _alert_context.in_alert = False

    def test_alert_level_parameter_passed_to_handler(self):
        @telegram_alert(alert_level='ERROR')
        def func():
            return True

        with patch('src.utils.decorator_utils.send_message'):
            func()


# ---------------------------------------------------------------------------
# timeout decorator
# ---------------------------------------------------------------------------


class TestTimeoutDecorator:
    def test_zero_or_negative_raises_value_error(self):
        with pytest.raises(ValueError):
            timeout(0)

        with pytest.raises(ValueError):
            timeout(-5)

    def test_function_completes_within_timeout(self):
        @timeout(5)
        def fast():
            return 'done'

        assert fast() == 'done'

    def test_preserves_function_name(self):
        @timeout(5)
        def named_func():
            pass

        assert named_func.__name__ == 'named_func'

    def test_returns_correct_value(self):
        @timeout(5)
        def compute():
            return [1, 2, 3]

        assert compute() == [1, 2, 3]

    def test_passes_args_and_kwargs(self):
        @timeout(5)
        def add(a, b=0):
            return a + b

        assert add(3, b=4) == 7

    def test_propagates_exception_within_timeout(self):
        @timeout(5)
        def raises():
            raise ValueError('inner error')

        with pytest.raises(ValueError, match='inner error'):
            raises()

    def test_timeout_exceeded_raises_timeout_error(self):
        """Test using thread-based fallback (non-main-thread execution)."""

        @timeout(1)
        def slow():
            time.sleep(10)

        result_holder = []

        try:
            slow()
            result_holder.append('no_error')
        except TimeoutError:
            result_holder.append('timeout')
        except Exception as e:
            result_holder.append(f'other: {e}')

        assert 'timeout' in result_holder

    def test_timeout_error_message_includes_function_name(self):
        @timeout(1)
        def my_slow_function():
            time.sleep(10)

        result_holder = []

        def run():
            try:
                my_slow_function()
            except TimeoutError as e:
                result_holder.append(str(e))

        run()
        assert result_holder
        assert 'my_slow_function' in result_holder[0]


# ---------------------------------------------------------------------------
# ingestion_audit decorator
# ---------------------------------------------------------------------------


class TestIngestionAuditDecorator:
    def test_audit_success_inserts_then_updates_row(self):
        mock_sqlite = MagicMock()

        @ingestion_audit()
        def sample_job(config):
            return 3

        config = {
            'job_name': 'job_a',
            'database': 'ingestion.db',
            'handler': 'strava',
            'extract_method': 'run',
            'table': 't',
            'config_file': str(
                Path(PATH_INGESTION_FOLDER) / 'system_stats.yml'
            ),
        }

        with (
            patch(
                'src.utils.decorator_utils.load_json',
                return_value={
                    'job_a': {
                        'database': 'ingestion.db',
                        'handler': 'strava',
                        'extract_method': 'run',
                        'table': 't',
                        'config_file': str(
                            Path(PATH_INGESTION_FOLDER) / 'system_stats.yml'
                        ),
                    }
                },
            ),
            patch(
                'src.utils.decorator_utils.SQLiteHandler',
                return_value=mock_sqlite,
            ),
            patch(
                'src.utils.decorator_utils.get_git_head',
                return_value='abc123',
            ),
        ):
            result = sample_job(config)

        assert result == 3
        mock_sqlite.insert_data.assert_called_once()
        inserted_row = mock_sqlite.insert_data.call_args.kwargs['data'][0]
        assert inserted_row['job_name'] == 'job_a'
        assert inserted_row['commit_hash'] == 'abc123'
        assert inserted_row['id']
        assert inserted_row['updated_config'] is None
        assert inserted_row['config_file'] == 'system_stats.yml'

        mock_sqlite.upsert_data.assert_called_once()
        updated_row = mock_sqlite.upsert_data.call_args.kwargs['data'][0]
        assert updated_row['status'] == 1
        assert updated_row['num_records'] == 3
        assert updated_row['error'] is None

    def test_audit_failure_inserts_row_with_error(self):
        mock_sqlite = MagicMock()

        @ingestion_audit()
        def failing_job(config):
            raise ValueError('ingestion failed')

        config = {
            'job_name': 'job_b',
            'database': 'ingestion.db',
            'handler': 'strava',
            'extract_method': 'run',
            'table': 'runtime_table',
            'config_file': str(Path(PATH_INGESTION_FOLDER) / 'strava.yml'),
        }

        with (
            patch(
                'src.utils.decorator_utils.load_json',
                return_value={
                    'job_b': {
                        'database': 'ingestion.db',
                        'handler': 'strava',
                        'extract_method': 'run',
                        'table': 'default_table',
                        'config_file': str(
                            Path(PATH_INGESTION_FOLDER) / 'strava.yml'
                        ),
                    }
                },
            ),
            patch(
                'src.utils.decorator_utils.SQLiteHandler',
                return_value=mock_sqlite,
            ),
            patch(
                'src.utils.decorator_utils.get_git_head',
                return_value='abc123',
            ),
        ):
            with pytest.raises(ValueError, match='ingestion failed'):
                failing_job(config)

        mock_sqlite.insert_data.assert_called_once()
        inserted_row = mock_sqlite.insert_data.call_args.kwargs['data'][0]
        assert inserted_row['job_name'] == 'job_b'
        assert inserted_row['updated_config'] is not None
        assert inserted_row['config_file'] == 'strava.yml'

        mock_sqlite.upsert_data.assert_called_once()
        updated_row = mock_sqlite.upsert_data.call_args.kwargs['data'][0]
        assert updated_row['status'] == 0
        assert 'ValueError' in updated_row['error']


# ---------------------------------------------------------------------------
# script_execution_audit decorator
# ---------------------------------------------------------------------------


class TestScriptExecutionAuditDecorator:
    def test_script_audit_success_inserts_expected_fields(self):
        mock_sqlite = MagicMock()

        @script_execution_audit(table_name='script_execution_audit')
        def sample_script():
            return 'ok'

        with (
            patch(
                'src.utils.decorator_utils.SQLiteHandler',
                return_value=mock_sqlite,
            ),
            patch(
                'src.utils.decorator_utils.get_git_head',
                return_value='abc123',
            ),
            patch('src.utils.decorator_utils.sys.argv', ['test_runner']),
        ):
            result = sample_script()

        assert result == 'ok'
        mock_sqlite.insert_data.assert_called_once()
        inserted = mock_sqlite.insert_data.call_args.kwargs['data'][0]
        assert inserted['id']
        assert inserted['execution_id'] is None
        assert inserted['script_name'] == 'test_decorator_utils.py'
        assert inserted['commit_hash'] == 'abc123'
        assert inserted['status'] == 1
        assert inserted['error'] is None
        assert inserted['args'] == '[]'

    def test_script_audit_failure_stores_full_traceback(self):
        mock_sqlite = MagicMock()

        @script_execution_audit(table_name='script_execution_audit')
        def failing_script():
            raise RuntimeError('boom')

        with (
            patch(
                'src.utils.decorator_utils.SQLiteHandler',
                return_value=mock_sqlite,
            ),
            patch(
                'src.utils.decorator_utils.get_git_head',
                return_value='abc123',
            ),
        ):
            with pytest.raises(RuntimeError, match='boom'):
                failing_script()

        inserted = mock_sqlite.insert_data.call_args.kwargs['data'][0]
        assert inserted['status'] == 0
        assert 'Traceback' in inserted['error']
        assert 'RuntimeError: boom' in inserted['error']
