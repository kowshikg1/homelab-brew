"""Tests for src/handlers/base_api.py"""
import pytest
from unittest.mock import patch, MagicMock, call

from src.handlers.base_api import BaseAPI, APIEnvConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def env_config():
    return APIEnvConfig(
        CLIENT_ID="TEST_CLIENT_ID",
        CLIENT_SECRET="TEST_CLIENT_SECRET",
        REFRESH_TOKEN="TEST_REFRESH_TOKEN",
        ACCESS_TOKEN="TEST_ACCESS_TOKEN",
        EXPIRES_AT="TEST_EXPIRES_AT",
    )


@pytest.fixture
def mock_env_manager():
    mock_mgr = MagicMock()
    with patch("src.handlers.base_api.EnvManager", return_value=mock_mgr):
        yield mock_mgr


# ---------------------------------------------------------------------------
# APIEnvConfig
# ---------------------------------------------------------------------------

class TestAPIEnvConfig:
    def test_all_fields_stored(self):
        cfg = APIEnvConfig(
            CLIENT_ID="CID",
            CLIENT_SECRET="CSECRET",
            REFRESH_TOKEN="RTOKEN",
            ACCESS_TOKEN="ATOKEN",
            EXPIRES_AT="EXPAT",
        )
        assert cfg.CLIENT_ID == "CID"
        assert cfg.CLIENT_SECRET == "CSECRET"
        assert cfg.REFRESH_TOKEN == "RTOKEN"
        assert cfg.ACCESS_TOKEN == "ATOKEN"
        assert cfg.EXPIRES_AT == "EXPAT"


# ---------------------------------------------------------------------------
# BaseAPI initialisation
# ---------------------------------------------------------------------------

class TestBaseAPIInit:
    def test_defaults_to_none(self):
        api = BaseAPI()
        assert api._base_url is None
        assert api._client_id is None
        assert api._client_secret is None
        assert api._refresh_token is None
        assert api._token_url is None

    def test_stores_constructor_args(self):
        api = BaseAPI(
            base_url="https://api.example.com",
            client_id="cid",
            client_secret="csecret",
            refresh_token="rtoken",
            token_url="https://token.example.com",
        )
        assert api._base_url == "https://api.example.com"
        assert api._client_id == "cid"
        assert api._client_secret == "csecret"
        assert api._refresh_token == "rtoken"
        assert api._token_url == "https://token.example.com"


# ---------------------------------------------------------------------------
# lazy properties
# ---------------------------------------------------------------------------

class TestBaseAPILazyProperties:
    def test_base_url_lazy(self):
        api = BaseAPI(base_url="https://example.com")
        assert api.base_url == "https://example.com"

    def test_client_id_lazy(self):
        api = BaseAPI(client_id="my_id")
        assert api.client_id == "my_id"

    def test_client_secret_lazy(self):
        api = BaseAPI(client_secret="secret")
        assert api.client_secret == "secret"

    def test_refresh_token_lazy(self):
        api = BaseAPI(refresh_token="rtoken")
        assert api.refresh_token == "rtoken"

    def test_token_url_lazy(self):
        api = BaseAPI(token_url="https://token.url")
        assert api.token_url == "https://token.url"


# ---------------------------------------------------------------------------
# access_token — abstract
# ---------------------------------------------------------------------------

class TestBaseAPIAccessToken:
    def test_access_token_raises_not_implemented(self):
        api = BaseAPI()
        with pytest.raises(NotImplementedError):
            api.access_token()


# ---------------------------------------------------------------------------
# get_access_token_OAuth2
# ---------------------------------------------------------------------------

class TestGetAccessTokenOAuth2:
    def _make_api(self):
        api = BaseAPI(token_url="https://token.example.com")
        return api

    def test_returns_current_token_when_not_expired(self, env_config):
        """If expires_at is None (not set), should return existing access token without refresh."""
        mock_mgr = MagicMock()
        # expires_at is None → no refresh needed
        mock_mgr.get.side_effect = lambda key, default=None: {
            env_config.EXPIRES_AT: None,
            env_config.ACCESS_TOKEN: "valid_token",
        }.get(key, default)

        with patch("src.handlers.base_api.EnvManager", return_value=mock_mgr):
            api = self._make_api()
            token = api.get_access_token_OAuth2(env_config)

        assert token == "valid_token"

    def test_refreshes_token_when_expired(self, env_config):
        import time
        past_time = str(int(time.time()) - 100)  # expired 100s ago

        mock_mgr = MagicMock()
        values = {
            env_config.EXPIRES_AT: past_time,
            env_config.REFRESH_TOKEN: "old_refresh",
            env_config.CLIENT_ID: "client_id",
            env_config.CLIENT_SECRET: "client_secret",
            env_config.ACCESS_TOKEN: "new_access_token",
        }
        mock_mgr.get.side_effect = lambda key, default=None: values.get(key, default)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh",
            "expires_at": str(int(time.time()) + 3600),
            "expires_in": 3660,
        }

        with patch("src.handlers.base_api.EnvManager", return_value=mock_mgr), \
             patch("src.handlers.base_api.requests.post", return_value=mock_response) as mock_post:
            api = self._make_api()
            token = api.get_access_token_OAuth2(env_config)

        mock_post.assert_called_once()
        assert token == "new_access_token"

    def test_refresh_sends_correct_payload(self, env_config):
        import time
        past_time = str(int(time.time()) - 100)

        mock_mgr = MagicMock()
        values = {
            env_config.EXPIRES_AT: past_time,
            env_config.REFRESH_TOKEN: "my_refresh",
            env_config.CLIENT_ID: "my_client",
            env_config.CLIENT_SECRET: "my_secret",
            env_config.ACCESS_TOKEN: "new_token",
        }
        mock_mgr.get.side_effect = lambda key, default=None: values.get(key, default)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_token",
            "expires_in": 3600,
        }

        with patch("src.handlers.base_api.EnvManager", return_value=mock_mgr), \
             patch("src.handlers.base_api.requests.post", return_value=mock_response) as mock_post:
            api = self._make_api()
            api.get_access_token_OAuth2(env_config)

        call_kwargs = mock_post.call_args[1]
        data = call_kwargs["data"]
        assert data["client_id"] == "my_client"
        assert data["client_secret"] == "my_secret"
        assert data["grant_type"] == "refresh_token"
        assert data["refresh_token"] == "my_refresh"

    def test_no_refresh_when_not_expired(self, env_config):
        import time
        future_time = str(int(time.time()) + 7200)

        mock_mgr = MagicMock()
        values = {
            env_config.EXPIRES_AT: future_time,
            env_config.ACCESS_TOKEN: "still_valid",
        }
        mock_mgr.get.side_effect = lambda key, default=None: values.get(key, default)

        with patch("src.handlers.base_api.EnvManager", return_value=mock_mgr), \
             patch("src.handlers.base_api.requests.post") as mock_post:
            api = self._make_api()
            token = api.get_access_token_OAuth2(env_config)

        mock_post.assert_not_called()
        assert token == "still_valid"
