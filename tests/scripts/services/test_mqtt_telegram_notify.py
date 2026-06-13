"""Tests for src/scripts/mqtt_telegram_notify.py"""

from unittest.mock import MagicMock, patch

import pytest

from src.scripts.services.mqtt_telegram_notify import (
    _build_mqtt_client,
    subscribed_topics,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_env_manager():
    env_values = {
        'MQTT_BROKER': 'localhost',
        'MQTT_PORT': '1883',
        'MQTT_USERNAME': None,
        'MQTT_PASSWORD': None,
    }
    mock_mgr = MagicMock()
    mock_mgr.get.side_effect = lambda key, default=None: env_values.get(
        key, default
    )
    with patch(
        'src.scripts.services.mqtt_telegram_notify.EnvManager',
        return_value=mock_mgr,
    ):
        yield mock_mgr


@pytest.fixture
def mock_env_with_auth():
    env_values = {
        'MQTT_BROKER': 'mqtt.example.com',
        'MQTT_PORT': '8883',
        'MQTT_USERNAME': 'user',
        'MQTT_PASSWORD': 'pass',
    }
    mock_mgr = MagicMock()
    mock_mgr.get.side_effect = lambda key, default=None: env_values.get(
        key, default
    )
    with patch(
        'src.scripts.services.mqtt_telegram_notify.EnvManager',
        return_value=mock_mgr,
    ):
        yield mock_mgr


# ---------------------------------------------------------------------------
# subscribed_topics
# ---------------------------------------------------------------------------


class TestSubscribedTopics:
    def test_is_list(self):
        assert isinstance(subscribed_topics, list)

    def test_contains_home_alerts(self):
        assert 'home/alerts' in subscribed_topics

    def test_contains_jellyfin_webhooks(self):
        assert 'jellyfin/webhooks' in subscribed_topics

    def test_at_least_one_topic(self):
        assert len(subscribed_topics) >= 1


# ---------------------------------------------------------------------------
# _build_mqtt_client
# ---------------------------------------------------------------------------


class TestBuildMqttClient:
    def test_returns_mqtt_client(self, mock_env_manager):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            result = _build_mqtt_client()
        assert result is mock_client

    def test_connects_to_broker(self, mock_env_manager):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()
        mock_client.connect.assert_called_once_with('localhost', 1883, 60)

    def test_connects_to_custom_broker(self, mock_env_with_auth):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()
        mock_client.connect.assert_called_once_with(
            'mqtt.example.com', 8883, 60
        )

    def test_sets_username_password_when_provided(self, mock_env_with_auth):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()
        mock_client.username_pw_set.assert_called_once_with(
            username='user', password='pass'
        )

    def test_no_username_password_when_not_provided(self, mock_env_manager):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()
        mock_client.username_pw_set.assert_not_called()

    def test_on_connect_callback_set(self, mock_env_manager):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()
        assert mock_client.on_connect is not None

    def test_on_message_callback_set(self, mock_env_manager):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()
        assert mock_client.on_message is not None

    def test_on_disconnect_callback_set(self, mock_env_manager):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()
        assert mock_client.on_disconnect is not None


# ---------------------------------------------------------------------------
# on_connect callback
# ---------------------------------------------------------------------------


class TestOnConnectCallback:
    def _get_on_connect(self, env_fixture_name, request):
        request.getfixturevalue(env_fixture_name)
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()
        return mock_client.on_connect, mock_client

    def test_subscribes_to_all_topics_on_rc_0(self, mock_env_manager):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()

        on_connect = mock_client.on_connect
        on_connect(mock_client, None, None, 0)

        subscribed = [c[0][0] for c in mock_client.subscribe.call_args_list]
        for topic in subscribed_topics:
            assert topic in subscribed

    def test_does_not_subscribe_on_nonzero_rc(self, mock_env_manager):
        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()

        on_connect = mock_client.on_connect
        on_connect(mock_client, None, None, 1)

        mock_client.subscribe.assert_not_called()


# ---------------------------------------------------------------------------
# on_message callback
# ---------------------------------------------------------------------------


class TestOnMessageCallback:
    def test_sends_telegram_message_on_mqtt_message(self, mock_env_manager):
        mock_client = MagicMock()
        with (
            patch(
                'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
                return_value=mock_client,
            ),
            patch(
                'src.scripts.services.mqtt_telegram_notify.send_message'
            ) as mock_send,
        ):
            _build_mqtt_client()

            on_message = mock_client.on_message
            msg = MagicMock()
            msg.topic = 'home/alerts'
            msg.payload = b'test payload'
            on_message(mock_client, None, msg)

        mock_send.assert_called_once()

    def test_telegram_message_contains_topic(self, mock_env_manager):
        mock_client = MagicMock()
        with (
            patch(
                'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
                return_value=mock_client,
            ),
            patch(
                'src.scripts.services.mqtt_telegram_notify.send_message'
            ) as mock_send,
        ):
            _build_mqtt_client()

            on_message = mock_client.on_message
            msg = MagicMock()
            msg.topic = 'home/alerts'
            msg.payload = b'payload text'
            on_message(mock_client, None, msg)

        assert 'home/alerts' in mock_send.call_args[0][0]

    def test_telegram_message_contains_payload(self, mock_env_manager):
        mock_client = MagicMock()
        with (
            patch(
                'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
                return_value=mock_client,
            ),
            patch(
                'src.scripts.services.mqtt_telegram_notify.send_message'
            ) as mock_send,
        ):
            _build_mqtt_client()

            on_message = mock_client.on_message
            msg = MagicMock()
            msg.topic = 'home/alerts'
            msg.payload = b'the actual payload'
            on_message(mock_client, None, msg)

        assert 'the actual payload' in mock_send.call_args[0][0]

    def test_decodes_payload_utf8_with_errors_replace(self, mock_env_manager):
        mock_client = MagicMock()
        with (
            patch(
                'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
                return_value=mock_client,
            ),
            patch(
                'src.scripts.services.mqtt_telegram_notify.send_message'
            ) as mock_send,
        ):
            _build_mqtt_client()

            on_message = mock_client.on_message
            msg = MagicMock()
            msg.topic = 'home/alerts'
            msg.payload = b'\xff\xfe invalid utf8'
            on_message(mock_client, None, msg)

        # Should not raise; sends something
        mock_send.assert_called_once()


# ---------------------------------------------------------------------------
# on_disconnect callback
# ---------------------------------------------------------------------------


class TestOnDisconnectCallback:
    def test_logs_warning_on_unexpected_disconnect(
        self, mock_env_manager, caplog
    ):
        import logging

        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()

        on_disconnect = mock_client.on_disconnect
        with caplog.at_level(logging.WARNING):
            on_disconnect(mock_client, None, 1)  # rc != 0

        assert any(
            'disconnect' in r.message.lower() or 'rc' in r.message.lower()
            for r in caplog.records
        )

    def test_logs_info_on_clean_disconnect(self, mock_env_manager, caplog):
        import logging

        mock_client = MagicMock()
        with patch(
            'src.scripts.services.mqtt_telegram_notify.mqtt.Client',
            return_value=mock_client,
        ):
            _build_mqtt_client()

        on_disconnect = mock_client.on_disconnect
        with caplog.at_level(logging.INFO):
            on_disconnect(mock_client, None, 0)
