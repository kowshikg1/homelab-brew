import functools
import logging
import signal
import traceback
import threading

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Any, Callable

from src.handlers.telegram import send_message
from src.utils.log_util import get_logger

log = get_logger(__name__)

# Thread-local storage to prevent infinite loops
_alert_context = threading.local()


class _TelegramAlertHandler(logging.Handler):
    """Custom logging handler that sends Telegram alerts for specific log levels."""
    
    def __init__(self, alert_level: str = "WARNING"):
        super().__init__()
        self.alert_level = getattr(logging, alert_level.upper(), logging.WARNING)
    
    def emit(self, record: logging.LogRecord):
        """Send Telegram alert if log level meets threshold."""
        # Prevent infinite loops
        if getattr(_alert_context, "in_alert", False):
            return
        
        try:
            if record.levelno >= self.alert_level:
                message = f"🔔 *Log Alert*\n*Level:* {record.levelname}\n*Logger:* {record.name}\n*Message:* {record.getMessage()}"
                
                if record.exc_info:
                    exc_str = "".join(traceback.format_exception(*record.exc_info))
                    message += f"\n\n*Traceback:*\n```\n{exc_str}\n```"
                
                _alert_context.in_alert = True
                try:
                    send_message(message)
                finally:
                    _alert_context.in_alert = False
        except Exception as e:
            # Silently fail to prevent decorator from breaking the function
            pass


def telegram_alert(alert_level: str = "WARNING") -> Callable:
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
            if getattr(_alert_context, "in_alert", False):
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
                        f"❌ *Error in Function*\n"
                        f"*Function:* `{func_module}.{func_name}`\n"
                        f"*Exception:* `{type(e).__name__}`\n"
                        f"*Message:* {str(e)}\n\n"
                        f"*Full Traceback:*\n"
                        f"```\n{exc_traceback}\n```"
                    )
                    
                    send_message(message)
                    log.error(f"Error in {func_module}.{func_name}: {exc_traceback}")
                except Exception as alert_error:
                    # If alert fails, log it but don't break the outer error handling
                    log.error(f"Failed to send Telegram alert: {alert_error}")
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
        raise ValueError("timeout seconds must be > 0")

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            in_main_thread = threading.current_thread() is threading.main_thread()

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
                