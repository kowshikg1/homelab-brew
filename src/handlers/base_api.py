from dataclasses import dataclass
from pathlib import Path

import requests
from lazy import lazy

from src.handlers.env_manager import EnvManager
from src.utils.commons import current_timestamp
from src.utils.log_util import get_logger
from src.utils.path_variables import ENV_FILE_HANDLERS

log = get_logger(Path(__file__).stem)


@dataclass
class APIEnvConfig:
    """Data class to hold API environment variable keys"""

    CLIENT_ID: str
    CLIENT_SECRET: str
    REFRESH_TOKEN: str
    ACCESS_TOKEN: str
    EXPIRES_AT: str


class BaseAPI:
    def __init__(
        self,
        base_url: str = None,
        client_id: str = None,
        client_secret: str = None,
        refresh_token: str = None,
        token_url: str = None,
    ) -> None:
        self._base_url = base_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._token_url = token_url

    @lazy
    def base_url(self):
        return self._base_url

    @lazy
    def client_id(self):
        return self._client_id

    @lazy
    def client_secret(self):
        return self._client_secret

    @lazy
    def refresh_token(self):
        return self._refresh_token

    @lazy
    def token_url(self):
        return self._token_url

    def access_token(self):
        raise NotImplementedError(
            'Subclasses must implement access_token method'
        )

    def get_access_token_OAuth2(self, env_keys: APIEnvConfig) -> str:
        env_manager = EnvManager(ENV_FILE_HANDLERS)
        expires_at = env_manager.get(env_keys.EXPIRES_AT)

        if expires_at and current_timestamp() >= float(expires_at):
            refresh_token = env_manager.get(env_keys.REFRESH_TOKEN)
            client_id = env_manager.get(env_keys.CLIENT_ID)
            client_secret = env_manager.get(env_keys.CLIENT_SECRET)

            res = requests.post(
                self.token_url,
                data={
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token,
                },
                timeout=60,
            )
            if res.status_code == 200:
                data = res.json()
                env_manager.set(
                    env_keys.ACCESS_TOKEN,
                    data['access_token'],
                    **{
                        env_keys.REFRESH_TOKEN: data.get(
                            'refresh_token', refresh_token
                        )
                        or refresh_token,
                        env_keys.EXPIRES_AT: data.get(
                            'expires_at',
                            str(
                                current_timestamp()
                                + int(data['expires_in'] - 60)
                            ),
                        ),  # 60sec buffer
                    },
                )
        return env_manager.get(env_keys.ACCESS_TOKEN)
