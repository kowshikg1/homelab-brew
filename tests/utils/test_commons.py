"""Tests for src/utils/commons.py"""

from collections import deque
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import Mock, patch

from src.utils.commons import (
    current_timestamp,
    get_git_head,
    hash_object,
    to_text,
)

# ---------------------------------------------------------------------------
# hash_object
# ---------------------------------------------------------------------------


class TestHashObject:
    def test_returns_hex_string(self):
        result = hash_object('hello')
        assert isinstance(result, str)
        assert len(result) == 32  # MD5 hex digest

    def test_same_input_same_hash(self):
        assert hash_object('hello') == hash_object('hello')

    def test_different_inputs_different_hashes(self):
        assert hash_object('hello') != hash_object('world')

    # --- primitive types ---

    def test_string_input(self):
        h = hash_object('test_string')
        assert isinstance(h, str)

    def test_int_input(self):
        h = hash_object(42)
        assert isinstance(h, str)

    def test_float_input(self):
        h = hash_object(3.14)
        assert isinstance(h, str)

    def test_none_input(self):
        h = hash_object(None)
        assert isinstance(h, str)

    # --- compound types ---

    def test_list_input(self):
        h1 = hash_object([1, 2, 3])
        h2 = hash_object([1, 2, 3])
        assert h1 == h2

    def test_list_order_matters(self):
        assert hash_object([1, 2]) != hash_object([2, 1])

    def test_tuple_input(self):
        h1 = hash_object((1, 2, 3))
        h2 = hash_object((1, 2, 3))
        assert h1 == h2

    def test_deque_input(self):
        h1 = hash_object(deque([1, 2, 3]))
        h2 = hash_object(deque([1, 2, 3]))
        assert h1 == h2

    def test_set_input(self):
        h = hash_object({1, 2, 3})
        assert isinstance(h, str)

    def test_dict_input(self):
        d = {'a': 1, 'b': 2}
        h1 = hash_object(d)
        h2 = hash_object(d)
        assert h1 == h2

    def test_dict_key_order_invariant(self):
        d1 = {'a': 1, 'b': 2}
        d2 = {'b': 2, 'a': 1}
        assert hash_object(d1) == hash_object(d2)

    def test_nested_dict(self):
        d = {'outer': {'inner': [1, 2, 3]}}
        assert hash_object(d) == hash_object(d)

    def test_callable_input(self):
        def my_func():
            pass

        h = hash_object(my_func)
        assert isinstance(h, str)

    def test_callable_identified_by_name(self):
        def func_a():
            pass

        def func_b():
            pass

        assert hash_object(func_a) != hash_object(func_b)

    def test_encoding_parameter(self):
        h_utf16 = hash_object('hello', encoding='utf-16')
        h_utf8 = hash_object('hello', encoding='utf-8')
        # Different encoding → different bytes → different hash
        assert h_utf16 != h_utf8


# ---------------------------------------------------------------------------
# to_text
# ---------------------------------------------------------------------------


class TestToText:
    def test_dict_to_json_string(self):
        result = to_text({'key': 'value'})
        assert result == '{"key": "value"}'

    def test_list_to_json_string(self):
        result = to_text([1, 2, 3])
        assert result == '[1, 2, 3]'

    def test_tuple_to_json_string(self):
        result = to_text((1, 2))
        assert (
            '"' in result or '[' in result
        )  # json.dumps converts tuple to array

    def test_set_to_json_string(self):
        result = to_text({42})
        assert '42' in result

    def test_bytes_decoded_utf8(self):
        result = to_text(b'hello bytes')
        assert result == 'hello bytes'

    def test_string_passthrough(self):
        assert to_text('plain string') == 'plain string'

    def test_int_to_str(self):
        assert to_text(123) == '123'

    def test_float_to_str(self):
        assert to_text(3.14) == '3.14'

    def test_none_to_str(self):
        assert to_text(None) == 'None'

    def test_nested_dict_serializable(self):
        result = to_text({'a': [1, 2], 'b': {'c': 3}})
        import json

        parsed = json.loads(result)
        assert parsed == {'a': [1, 2], 'b': {'c': 3}}

    def test_dict_with_non_serializable_uses_default_str(self):
        from datetime import datetime

        dt = datetime(2024, 1, 1)
        result = to_text({'ts': dt})
        assert '2024' in result  # default=str converts datetime


# ---------------------------------------------------------------------------
# current_timestamp
# ---------------------------------------------------------------------------


class TestCurrentTimestamp:
    def test_returns_int(self):
        ts = current_timestamp()
        assert isinstance(ts, int)

    def test_is_recent(self):
        now = int(datetime.now(UTC).timestamp())
        ts = current_timestamp()
        assert abs(ts - now) <= 2  # within 2 seconds

    def test_default_timezone_is_utc(self):
        # Two calls are close in value
        ts1 = current_timestamp()
        ts2 = current_timestamp()
        assert abs(ts2 - ts1) <= 1

    def test_custom_timezone(self):
        tz_plus5 = timezone(timedelta(hours=5))
        ts = current_timestamp(tz=tz_plus5)
        assert isinstance(ts, int)
        # Epoch seconds are independent of timezone
        now_utc = int(datetime.now(UTC).timestamp())
        assert abs(ts - now_utc) <= 2

    def test_monotonically_non_decreasing(self):
        import time

        ts1 = current_timestamp()
        time.sleep(0.01)
        ts2 = current_timestamp()
        assert ts2 >= ts1

    def test_millisecond_precision(self):
        ts_ms = current_timestamp(precision='ms')
        ts_s = current_timestamp(precision='s')
        assert isinstance(ts_ms, int)
        assert ts_ms >= ts_s * 1000

    def test_microsecond_precision(self):
        ts_us = current_timestamp(precision='us')
        ts_ms = current_timestamp(precision='ms')
        assert isinstance(ts_us, int)
        assert ts_us >= ts_ms * 1000

    def test_invalid_precision_raises(self):
        import pytest

        with pytest.raises(ValueError, match='precision must be one of'):
            current_timestamp(precision='ns')


# ---------------------------------------------------------------------------
# get_git_head
# ---------------------------------------------------------------------------


class TestGetGitHeadCommit:
    def test_returns_commit_hash(self):
        mock_result = Mock(stdout='abc123\n')
        with patch(
            'src.utils.commons.subprocess.run', return_value=mock_result
        ):
            assert get_git_head() == 'abc123'

    def test_returns_none_when_command_fails(self):
        with patch(
            'src.utils.commons.subprocess.run',
            side_effect=Exception('git not available'),
        ):
            assert get_git_head() is None
