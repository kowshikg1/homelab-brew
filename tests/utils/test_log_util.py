"""Tests for src/utils/log_util.py"""
import logging
import pytest
from pathlib import Path

from src.utils.log_util import get_logger


class TestGetLogger:
    def test_returns_logger_instance(self):
        logger = get_logger("test.module")
        assert isinstance(logger, logging.Logger)

    def test_logger_name_set(self):
        logger = get_logger("my.custom.name")
        assert logger.name == "my.custom.name"

    def test_logger_has_handlers(self):
        logger = get_logger("test.has_handlers")
        assert len(logger.handlers) > 0

    def test_logger_level_is_debug(self):
        logger = get_logger("test.level_debug")
        assert logger.level == logging.DEBUG

    def test_console_handler_present(self):
        logger = get_logger("test.console_handler")
        handler_types = [type(h) for h in logger.handlers]
        assert logging.StreamHandler in handler_types

    def test_idempotent_no_duplicate_handlers(self):
        name = "test.idempotent_unique_name_xyz"
        logger1 = get_logger(name)
        count_after_first = len(logger1.handlers)
        logger2 = get_logger(name)
        assert len(logger2.handlers) == count_after_first

    def test_same_name_returns_same_logger(self):
        name = "test.same_instance"
        l1 = get_logger(name)
        l2 = get_logger(name)
        assert l1 is l2

    def test_file_handler_added_when_log_file_given(self, tmp_path):
        log_file = str(tmp_path / "app.log")
        logger = get_logger("test.file_handler_unique", log_file=log_file)
        handler_types = [type(h) for h in logger.handlers]
        assert logging.FileHandler in handler_types

    def test_log_file_is_created(self, tmp_path):
        log_file = str(tmp_path / "logs" / "app.log")
        logger = get_logger("test.file_created_unique", log_file=log_file)
        logger.info("test message")
        assert Path(log_file).exists()

    def test_log_file_parent_dirs_created(self, tmp_path):
        log_file = str(tmp_path / "deep" / "nested" / "dir" / "app.log")
        get_logger("test.deep_dirs_unique", log_file=log_file)
        assert Path(log_file).parent.exists()

    def test_no_file_handler_when_log_file_none(self):
        logger = get_logger("test.no_file_handler_unique_999")
        handler_types = [type(h) for h in logger.handlers]
        assert logging.FileHandler not in handler_types

    def test_formatter_set_on_console_handler(self):
        logger = get_logger("test.formatter_check_unique")
        for h in logger.handlers:
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
                assert h.formatter is not None
                fmt_str = h.formatter._fmt
                assert "%(asctime)s" in fmt_str
                assert "%(levelname)s" in fmt_str
                break
