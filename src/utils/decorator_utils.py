import functools
import json
import logging
import signal
import sys
import threading
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from pathlib import Path
from typing import Any

from src.handlers.sqlite import SQLiteHandler
from src.handlers.telegram import send_message
from src.utils.commons import current_timestamp, get_git_head, hash_object
from src.utils.file import load_json
from src.utils.log_util import get_logger
from src.utils.path_variables import (
    PATH_INGESTION_CONFIG,
    PATH_INGESTION_FOLDER,
)

log = get_logger(__name__)

DEFAULT_TIME_PRECISION = (
    'ms'  # if precision changes, update src/monitor/handlers/jobs.py
)

# Thread-local storage to prevent infinite loops
_alert_context = threading.local()


def script_execution_audit(
    table_name: str = 'script_execution_audit',
    db_path: str = './data/ingestion.db',
) -> Callable:
    """Decorator to persist script execution audit records."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            execution_id = None  # Unique identifier for the script execution, currently null
            script_name = Path(func.__code__.co_filename).name
            start_time = current_timestamp(precision=DEFAULT_TIME_PRECISION)
            commit_hash = get_git_head()

            if args or kwargs:
                args_payload: Any = {
                    'args': list(args),
                    'kwargs': kwargs,
                }
            else:
                args_payload = sys.argv[1:]

            audit_id = hash_object(
                (script_name, start_time, args_payload, commit_hash)
            )
            end_time = None
            status = 0
            error = None

            try:
                result = func(*args, **kwargs)
                status = 1
                return result
            except Exception:
                error = traceback.format_exc()
                raise
            finally:
                end_time = current_timestamp(precision=DEFAULT_TIME_PRECISION)
                try:
                    sqlite_handler = SQLiteHandler(db_path)
                    sqlite_handler.insert_data(
                        table_name=table_name,
                        data=[
                            {
                                'id': audit_id,
                                'execution_id': execution_id,
                                'script_name': script_name,
                                'commit_hash': commit_hash,
                                'args': json.dumps(args_payload, default=str),
                                'start_time': start_time,
                                'end_time': end_time,
                                'status': status,
                                'error': error,
                            }
                        ],
                    )
                except Exception as exc:
                    log.warning(
                        f'Failed to insert script audit row into {table_name}: {exc}'
                    )

        return wrapper

    return decorator


def ingestion_audit(table_name: str = 'ingest_audit') -> Callable:
    """Decorator to persist ingestion execution audit records."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            run_id = None
            runtime_config = (
                args[0].copy() if args else kwargs.get('config', {}).copy()
            )
            job_name = runtime_config.pop(
                'job_name'
            )  # remove job_name from runtime_config as it's populated
            start_time = current_timestamp(precision=DEFAULT_TIME_PRECISION)
            commit_hash = get_git_head()
            id = hash_object((run_id, job_name, start_time, commit_hash))
            default_config = load_json(PATH_INGESTION_CONFIG).get(
                job_name, None
            )
            updated_config = (
                runtime_config
                if hash_object(runtime_config) != hash_object(default_config)
                else None
            )
            config_file = runtime_config.get('config_file', None)
            if config_file:
                config_file = (
                    Path(config_file)
                    .relative_to(PATH_INGESTION_FOLDER)
                    .as_posix()
                )
            audit_data = {
                'id': id,
                'run_id': run_id,
                'job_name': job_name,
                'commit_hash': commit_hash,
                'updated_config': (
                    json.dumps(updated_config, default=str)
                    if isinstance(updated_config, dict)
                    else None
                ),
                'start_time': start_time,
                'end_time': None,
                'status': None,
                'error': None,
                'num_records': None,
                'config_file': config_file,
            }
            try:
                db_name = runtime_config.get('database', 'ingestion.db')
                if not db_name.endswith('.db'):
                    db_name = f'{db_name}.db'
                sqlite_handler = SQLiteHandler(f'./data/{db_name}')
                sqlite_handler.insert_data(
                    table_name=table_name,
                    data=[audit_data],
                )
            except Exception as exc:
                log.warning(
                    f'Failed to insert audit row into {table_name}: {exc}'
                )
            try:
                result = func(*args, **kwargs)
                audit_data['status'] = 1
                if isinstance(result, int):
                    audit_data['num_records'] = result
                else:
                    try:
                        audit_data['num_records'] = len(result)
                    except TypeError:
                        audit_data['num_records'] = 0
                return result
            except Exception:
                audit_data['error'] = traceback.format_exc()
                audit_data['status'] = 0
                raise
            finally:
                audit_data['end_time'] = current_timestamp(
                    precision=DEFAULT_TIME_PRECISION
                )
                try:
                    sqlite_handler = SQLiteHandler(f'./data/{db_name}')
                    sqlite_handler.upsert_data(
                        table_name=table_name,
                        data=[audit_data],
                        unique_key='id',
                    )
                except Exception as exc:
                    log.warning(
                        f'Failed to update audit row into {table_name}: {exc}'
                    )

        return wrapper

    return decorator


class _TelegramAlertHandler(logging.Handler):
    """Custom logging handler that sends Telegram alerts for specific log levels."""

    def __init__(self, alert_level: str = 'WARNING'):
        super().__init__()
        self.alert_level = getattr(
            logging, alert_level.upper(), logging.WARNING
        )

    def emit(self, record: logging.LogRecord):
        """Send Telegram alert if log level meets threshold."""
        # Prevent infinite loops
        if getattr(_alert_context, 'in_alert', False):
            return

        try:
            if record.levelno >= self.alert_level:
                message = f'🔔 *Log Alert*\n*Level:* {record.levelname}\n*Logger:* {record.name}\n*Message:* {record.getMessage()}'

                if record.exc_info:
                    exc_str = ''.join(
                        traceback.format_exception(*record.exc_info)
                    )
                    message += f'\n\n*Traceback:*\n```\n{exc_str}\n```'

                _alert_context.in_alert = True
                try:
                    send_message(message)
                finally:
                    _alert_context.in_alert = False
        except Exception:
            # Silently fail to prevent decorator from breaking the function
            pass


def telegram_alert(alert_level: str = 'WARNING') -> Callable:
    """
    Decorator that sends Telegram alerts on exceptions and high-level logs.

    Args:
        alert_level: Minimum log level to alert on (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Features:
        - Sends full traceback on exceptions
        - Monitors log levels and sends alerts
        - Prevents infinite loops using thread-local context
        - Includes full file paths and error context
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Prevent infinite loops
            if getattr(_alert_context, 'in_alert', False):
                return func(*args, **kwargs)

            telegram_handler = _TelegramAlertHandler(alert_level)
            root_logger = logging.getLogger()
            root_logger.addHandler(telegram_handler)

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                # Mark that we're handling an alert to prevent infinite loops
                _alert_context.in_alert = True
                try:
                    exc_traceback = traceback.format_exc()
                    func_name = func.__name__
                    func_module = func.__module__

                    message = (
                        f'❌ *Error in Function*\n'
                        f'*Function:* `{func_module}.{func_name}`\n'
                        f'*Exception:* `{type(e).__name__}`\n'
                        f'*Message:* {str(e)}\n\n'
                        f'*Full Traceback:*\n'
                        f'```\n{exc_traceback}\n```'
                    )

                    send_message(message)
                    log.error(
                        f'Error in {func_module}.{func_name}: {exc_traceback}'
                    )
                except Exception as alert_error:
                    # If alert fails, log it but don't break the outer error handling
                    log.error(f'Failed to send Telegram alert: {alert_error}')
                finally:
                    _alert_context.in_alert = False

                # Re-raise the original exception
                raise
            finally:
                root_logger.removeHandler(telegram_handler)

        return wrapper

    return decorator


def timeout(seconds: int):
    """Decorator to enforce a timeout on synchronous functions.
    Use `wrapt-timeout-decorator` or `timeout-decorator` if using windows.

    On Unix main threads, this uses ``signal.setitimer`` (built-in and efficient).
    In other contexts (for example non-main threads), it falls back to a thread-based
    timeout. The fallback cannot force-stop the running function; it raises timeout in
    the caller while the worker thread may continue in the background.
    """
    if seconds <= 0:
        raise ValueError('timeout seconds must be > 0')

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            in_main_thread = (
                threading.current_thread() is threading.main_thread()
            )

            # Preferred built-in path for Linux/Unix in main thread.
            if in_main_thread:
                previous_handler = signal.getsignal(signal.SIGALRM)

                def _handle_timeout(signum, frame):
                    raise TimeoutError(
                        f"Function '{func.__module__}.{func.__name__}' timed out after {seconds}s"
                    )

                try:
                    signal.signal(signal.SIGALRM, _handle_timeout)
                    signal.setitimer(signal.ITIMER_REAL, float(seconds))
                    return func(*args, **kwargs)
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    signal.signal(signal.SIGALRM, previous_handler)

            # Fallback for non-main thread execution.
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except FuturesTimeoutError as exc:
                    raise TimeoutError(
                        f"Function '{func.__module__}.{func.__name__}' timed out after {seconds}s"
                    ) from exc

        return wrapper

    return decorator
