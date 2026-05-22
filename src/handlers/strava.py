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
    def header(self):
        return {"Authorization": f"Bearer {self.access_token}"}
    
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
        per_page = 50 #Could be up to 200
        activities: list[dict] = []
        page = 1

        while True:
            params = {
                # "before": ,
                "after": epoch_time,
                "page": page,
                "per_page": per_page,
            }
            res = requests.get(f"{self.base_url}/athlete/activities", headers=self.header, params=params, timeout=60)

            if res.status_code != 200:
                raise Exception(f"Failed to fetch activities: {res.status_code} - {res.text}")

            page_activities = res.json()
            if not page_activities:
                break

            activities.extend(page_activities)
            if len(page_activities) < per_page:
                break

            page += 1

        return activities
    
    def get_streams(self, activity_ids: list[int], keys: list[str]) -> dict:
        """Get the streams for a specific activity"""
        params = {"keys": ",".join(keys), "key_by_type": "true"}
        streams: list[dict] = []
        for activity_id in activity_ids:
            res = requests.get(f"{self.base_url}/activities/{activity_id}/streams", headers=self.header, params=params, timeout=60)
            if res.status_code == 200:
                row = {"id": activity_id}
                stream = res.json()
                for key in keys:
                    row[key] = stream.get(key)
                streams.append(row)
            else:
                raise Exception(f"Failed to fetch streams for activity {activity_id}: {res.status_code} - {res.text}")
        return streams

    def get_streams_helper(self, last_mtime:str =None, keys: list[str] =None) -> list[dict]:
        activities = self.get_activities(last_mtime=last_mtime)
        if not activities:
            return []
        activity_ids = [activity['id'] for activity in activities]
        start_dates = {activity['id']: activity['start_date'] for activity in activities}
        streams = self.get_streams(
            activity_ids, 
            keys=keys or ["time", "latlng", "distance", "altitude", "velocity_smooth", 
                "heartrate", "cadence", "watts", "temp", "moving", "grade_smooth"
            ]
        )
        for stream in streams:
            stream['start_date'] = start_dates.get(stream['id'])
        return streams


if __name__ == "__main__":
    # config = dict(
    #     id_config_col = "id",
    #     watermark_col = "start_date",
    # )
    import src.handlers.sqlite as sqlite
    db_handler = sqlite.SQLiteHandler()
    last_mtime = '2026-05-20T12:54:52Z'
    strava = Strava()
    streams = strava.get_streams_helper(last_mtime=last_mtime)
    db_handler.insert_data("strava_streams", streams)
    log.info(f"Ingestion Success. Fetched {len(streams)} streams")

