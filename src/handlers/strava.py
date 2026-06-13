from datetime import datetime
from pathlib import Path

import requests
from lazy import lazy

from src.handlers.base_api import APIEnvConfig, BaseAPI
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)


STRAVA_ENV_CONFIG = APIEnvConfig(
    CLIENT_ID='STRAVA_CLIENT_ID',
    CLIENT_SECRET='STRAVA_CLIENT_SECRET',
    REFRESH_TOKEN='STRAVA_REFRESH_TOKEN',
    ACCESS_TOKEN='STRAVA_ACCESS_TOKEN',
    EXPIRES_AT='STRAVA_EXPIRES_AT',
)


# ref: https://developers.strava.com/docs/reference/
class Strava(BaseAPI):
    # TODO: Pass evn variables through constructor and handle them in base class
    def __init__(self) -> None:
        self._base_url = 'https://www.strava.com/api/v3'
        self._token_url = 'https://www.strava.com/oauth/token'

    @lazy
    def header(self):
        return {'Authorization': f'Bearer {self.access_token}'}

    @lazy
    def access_token(self) -> str:
        return self.get_access_token_OAuth2(STRAVA_ENV_CONFIG)

    def get_activities(self, last_mtime: str = None) -> list:
        """Get the list of activities"""
        last_mtime = (
            datetime.fromisoformat(last_mtime.replace('Z', '+00:00'))
            if last_mtime
            else None
        )
        epoch_time = int(last_mtime.timestamp()) if last_mtime else None
        per_page = 50  # Could be up to 200
        activities: list[dict] = []
        page = 1

        while True:
            params = {
                # "before": ,
                'after': epoch_time,
                'page': page,
                'per_page': per_page,
            }
            res = requests.get(
                f'{self.base_url}/athlete/activities',
                headers=self.header,
                params=params,
                timeout=60,
            )

            if res.status_code != 200:
                raise Exception(
                    f'Failed to fetch activities: {res.status_code} - {res.text}'
                )

            page_activities = res.json()
            if not page_activities:
                break

            activities.extend(page_activities)
            if len(page_activities) < per_page:
                break

            page += 1

        return activities

    def get_streams(
        self, activity_ids: list[int], keys: list[str]
    ) -> list[dict]:
        """
        Get the streams for specific activities
        :param activity_ids: List of activity IDs to fetch streams for
        :param keys: List of stream types to fetch (e.g., "time", "latlng", "distance", etc.)

        """
        params = {'keys': ','.join(keys), 'key_by_type': 'true'}
        streams: list[dict] = []
        for activity_id in activity_ids:
            res = requests.get(
                f'{self.base_url}/activities/{activity_id}/streams',
                headers=self.header,
                params=params,
                timeout=60,
            )
            if res.status_code == 200:
                row = {'id': activity_id}
                stream = res.json()
                for key in keys:
                    row[key] = stream.get(key)
                streams.append(row)
            else:
                raise Exception(
                    f'Failed to fetch streams for activity {activity_id}: {res.status_code} - {res.text}'
                )
        return streams

    def get_streams_helper(
        self, last_mtime: str = None, keys: list[str] = None
    ) -> list[dict]:
        activities = self.get_activities(last_mtime=last_mtime)
        if not activities:
            return []
        activity_ids = [activity['id'] for activity in activities]
        start_dates = {
            activity['id']: activity['start_date'] for activity in activities
        }
        streams = self.get_streams(
            activity_ids,
            keys=keys
            or [
                'time',
                'latlng',
                'distance',
                'altitude',
                'velocity_smooth',
                'heartrate',
                'cadence',
                'watts',
                'temp',
                'moving',
                'grade_smooth',
            ],
        )
        for stream in streams:
            stream['start_date'] = start_dates.get(stream['id'])
        return streams


if __name__ == '__main__':
    # config = dict(
    #     id_config_col = "id",
    #     watermark_col = "start_date",
    # )
    import src.handlers.sqlite as sqlite

    db_handler = sqlite.SQLiteHandler()
    last_mtime = '2026-05-21T12:54:52Z'
    strava = Strava()
    streams = strava.get_activities(last_mtime=last_mtime)
    # db_handler.insert_data("strava_streams", streams)
    log.info(f'Ingestion Success. Fetched {len(streams)} streams')
