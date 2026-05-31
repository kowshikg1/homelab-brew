"""Tests for src/handlers/telegram.py"""
import pytest
from unittest.mock import patch, MagicMock

from src.handlers.telegram import send_message, get_chat_id


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_env_manager(tmp_path):
    """Patch EnvManager so tests never need a real .env file."""
    env_values = {
        "TELEGRAM_BOT_TOKEN": "test_bot_token",
        "TELEGRAM_CHAT_ID": "123456789",
    }

    mock_mgr = MagicMock()
    mock_mgr.get.side_effect = lambda key, default=None: env_values.get(key, default)

    with patch("src.handlers.telegram.EnvManager", return_value=mock_mgr):
        yield mock_mgr


# ---------------------------------------------------------------------------
# send_message
# ---------------------------------------------------------------------------

class TestSendMessage:
    def test_sends_post_request(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("src.handlers.telegram.requests.post", return_value=mock_response) as mock_post:
            send_message("Hello!")

        mock_post.assert_called_once()

    def test_url_contains_bot_token(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("src.handlers.telegram.requests.post", return_value=mock_response) as mock_post:
            send_message("Hello!")

        url = mock_post.call_args[0][0]
        assert "test_bot_token" in url

    def test_payload_contains_message(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("src.handlers.telegram.requests.post", return_value=mock_response) as mock_post:
            send_message("My message")

        payload = mock_post.call_args[1]["json"]
        assert payload["text"] == "My message"

    def test_payload_uses_env_chat_id_by_default(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("src.handlers.telegram.requests.post", return_value=mock_response) as mock_post:
            send_message("test")

        payload = mock_post.call_args[1]["json"]
        assert payload["chat_id"] == "123456789"

    def test_custom_chat_id_overrides_env(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("src.handlers.telegram.requests.post", return_value=mock_response) as mock_post:
            send_message("test", chat_id="custom_id")

        payload = mock_post.call_args[1]["json"]
        assert payload["chat_id"] == "custom_id"

    def test_parse_mode_is_markdown(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("src.handlers.telegram.requests.post", return_value=mock_response) as mock_post:
            send_message("test")

        payload = mock_post.call_args[1]["json"]
        assert payload["parse_mode"] == "Markdown"

    def test_logs_error_on_non_200_status(self, caplog):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"

        import logging
        with patch("src.handlers.telegram.requests.post", return_value=mock_response):
            with caplog.at_level(logging.ERROR):
                send_message("test")

        assert any("Failed" in r.message for r in caplog.records)

    def test_returns_none(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("src.handlers.telegram.requests.post", return_value=mock_response):
            result = send_message("hello")

        assert result is None


# ---------------------------------------------------------------------------
# get_chat_id
# ---------------------------------------------------------------------------

class TestGetChatId:
    def test_returns_chat_id_on_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "ok": True,
            "result": [{"message": {"chat": {"id": 987654321}}}],
        }

        with patch("src.handlers.telegram.requests.get", return_value=mock_response):
            result = get_chat_id()

        assert result == "987654321"

    def test_returns_none_when_result_empty(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True, "result": []}

        with patch("src.handlers.telegram.requests.get", return_value=mock_response):
            result = get_chat_id()

        assert result is None

    def test_returns_none_when_ok_is_false(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": False, "result": []}

        with patch("src.handlers.telegram.requests.get", return_value=mock_response):
            result = get_chat_id()

        assert result is None

    def test_logs_error_on_non_200(self, caplog):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        import logging
        with patch("src.handlers.telegram.requests.get", return_value=mock_response):
            with caplog.at_level(logging.ERROR):
                result = get_chat_id()

        assert result is None
        assert any("Failed" in r.message for r in caplog.records)

    def test_url_contains_bot_token(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "ok": True,
            "result": [{"message": {"chat": {"id": 1}}}],
        }

        with patch("src.handlers.telegram.requests.get", return_value=mock_response) as mock_get:
            get_chat_id()

        url = mock_get.call_args[0][0]
        assert "test_bot_token" in url
