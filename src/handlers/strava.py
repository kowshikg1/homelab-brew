import requests

from datetime import datetime, UTC
from enum import Enum
from lazy import lazy
from pathlib import Path

from src.handlers.env_manager import EnvManager
from src.utils.path_variables import ENV_FILE_HANDLERS
from src.utils.log_util import get_logger

log=get_logger(Path(__file__).stem)

class StravaConfig(Enum):
    """Strava configuration"""
    STRAVA_CLIENT_ID = "STRAVA_CLIENT_ID"
    STRAVA_CLIENT_SECRET = "STRAVA_CLIENT_SECRET"
    STRAVA_REFRESH_TOKEN = "STRAVA_REFRESH_TOKEN"
    STRAVA_ACCESS_TOKEN="STRAVA_ACCESS_TOKEN"
    STRAVA_EXPIRES_AT="STRAVA_EXPIRES_AT"


class Strava():
    def __init__(self) -> None:
        self.env_manager = EnvManager(ENV_FILE_HANDLERS)
        self.base_url = "https://www.strava.com/api/v3"
    
    @lazy
    def access_token(self)  -> str:
        current_timestamp = datetime.now(UTC).timestamp()
        expires_at = self.env_manager.get(StravaConfig.STRAVA_EXPIRES_AT.value)

        if expires_at and current_timestamp >= float(expires_at):
            refresh_token = self.refresh_token
            client_id = self.env_manager.get(StravaConfig.STRAVA_CLIENT_ID.value)
            client_secret = self.env_manager.get(StravaConfig.STRAVA_CLIENT_SECRET.value)

            res = requests.post(
                "https://www.strava.com/oauth/token",
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
            )
            if res.status_code == 200:
                data = res.json()
                self.env_manager.set(
                    StravaConfig.STRAVA_ACCESS_TOKEN.value, data["access_token"],
                    **{
                        StravaConfig.STRAVA_REFRESH_TOKEN.value: data["refresh_token"],
                        StravaConfig.STRAVA_EXPIRES_AT.value: str(data["expires_at"]),
                    }
                )
                return data["access_token"]
            else:
                raise Exception(f"Failed to refresh token: {res.status_code} - {res.text}")
        else:
            return self.env_manager.get(StravaConfig.STRAVA_ACCESS_TOKEN.value)
    
    @lazy
    def refresh_token(self) -> str:
        return self.env_manager.get(StravaConfig.STRAVA_REFRESH_TOKEN.value)

    def get_activities(self, last_mtime:str =None) -> list:
        """Get the list of activities"""
        last_mtime = datetime.fromisoformat(last_mtime.replace("Z", "+00:00")) if last_mtime else None
        epoch_time = int(last_mtime.timestamp()) if last_mtime else None

        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {
            # "before": ,
            "after": epoch_time,
            # page: 1,
            # "per_page": 200
            }
        res = requests.get(f"{self.base_url}/athlete/activities", headers=headers, params=params, timeout=60)

        if res.status_code == 200:
            return res.json()
        else:
            raise Exception(f"Failed to fetch activities: {res.status_code} - {res.text}")


if __name__ == "__main__":
    config = dict(
        id_config_col = "id",
        watermark_col = "start_date",
    )
    import src.handlers.sqlite as sqlite
    db_handler = sqlite.SQLiteHandler("ingestion.db")
    last_mtime = db_handler.get_last_mtime("strava_activities", watermark_col=config["watermark_col"])
    strava = Strava()
    activities = strava.get_activities(last_mtime=last_mtime)
    db_handler.insert_data("strava_activities", activities)
    log.info(f"Ingestion Success. Fetched {len(activities)} activities")

