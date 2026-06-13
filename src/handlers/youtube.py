from pathlib import Path

import requests
from lazy import lazy

from src.handlers.base_api import APIEnvConfig, BaseAPI
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)

YOUTUBE_ENV_CONFIG = APIEnvConfig(
    CLIENT_ID='YOUTUBE_CLIENT_ID',
    CLIENT_SECRET='YOUTUBE_CLIENT_SECRET',
    REFRESH_TOKEN='YOUTUBE_REFRESH_TOKEN',
    ACCESS_TOKEN='YOUTUBE_ACCESS_TOKEN',
    EXPIRES_AT='YOUTUBE_EXPIRES_AT',
)


# ref: https://developers.google.com/youtube/v3/docs
class Youtube(BaseAPI):
    def __init__(self) -> None:
        # TODO: Handle evn variables seperately in configs
        self._base_url = 'https://www.googleapis.com/youtube/v3'
        self._token_url = 'https://oauth2.googleapis.com/token'

    @lazy
    def header(self):
        return {'Authorization': f'Bearer {self.access_token}'}

    @lazy
    def access_token(self) -> str:
        return self.get_access_token_OAuth2(YOUTUBE_ENV_CONFIG)

    # TODO:use iterators for all below methods
    def get_playlists(self, channel_id: str = None, mine: bool = False) -> list:
        """Get the list of playlists for a channel or for the authenticated user if mine=True"""
        if mine and channel_id:
            raise ValueError('Cannot use channel_id and mine together.')
        playlists: list[dict] = []
        page_token = None

        while True:
            params = {
                'part': 'id,snippet,status,contentDetails',
                **({'channelId': channel_id} if channel_id else {}),
                **({'mine': mine} if mine else {}),
                'maxResults': 50,  # max allowed
                'pageToken': page_token,
            }
            res = requests.get(
                f'{self.base_url}/playlists',
                headers=self.header,
                params=params,
                timeout=60,
            )

            if res.status_code != 200:
                raise Exception(
                    f'Failed to fetch playlists: {res.status_code} - {res.text}'
                )

            data = res.json()
            playlists.extend(data.get('items', []))
            page_token = data.get('nextPageToken')

            if not page_token:
                break

        return playlists

    def get_playlist_ids(
        self, channel_ids: list[str] = None, mine: bool = False
    ) -> list[str]:
        """Get the playlist ids for a list of channel ids"""
        if not mine and not channel_ids:
            return []
        playlist_ids = []
        if mine:
            playlists = self.get_playlists(mine=True)
            playlist_ids.extend([playlist['id'] for playlist in playlists])
        if channel_ids:
            for channel_id in channel_ids:
                playlists = self.get_playlists(channel_id=channel_id)
                playlist_ids.extend([playlist['id'] for playlist in playlists])
        return list(set(playlist_ids))

    def get_playlist_items(self, playlist_id: str) -> list:
        """Get the list of items in a playlist"""
        items: list[dict] = []

        page_token = None

        while True:
            params = {
                'part': 'snippet',
                'playlistId': playlist_id,
                # "id": ','.join(playlist_ids), #for some reason not working, returns empty list.
                'maxResults': 50,  # max allowed
                'pageToken': page_token,
            }
            res = requests.get(
                f'{self.base_url}/playlistItems',
                headers=self.header,
                params=params,
                timeout=60,
            )

            if res.status_code != 200:
                raise Exception(
                    f'Failed to fetch playlist items: {res.status_code} - {res.text}'
                )

            data = res.json()
            items.extend(data.get('items', []))
            page_token = data.get('nextPageToken')

            if not page_token:
                break

        return items

    def get_playlist_items_helper(
        self,
        playlist_ids: list[str] = None,
        channel_ids: list[str] = None,
        mine: bool = False,
    ) -> list[dict]:
        playlist_ids = playlist_ids or []
        channel_ids = channel_ids or []
        playlist_ids.extend(
            self.get_playlist_ids(channel_ids=channel_ids, mine=mine)
        )
        all_items = []
        for playlist_id in set(playlist_ids):
            items = self.get_playlist_items(playlist_id=playlist_id)
            all_items.extend(items)
        return all_items


if __name__ == '__main__':
    playlist_ids = [
        'PLktEpJrBR-QmLlnjbx5cDmWSTxf0ZQa3c',
        'PLktEpJrBR-Ql7f9VBZN3C-De1eNjAzcS8',
        'PLktEpJrBR-Ql2KbrSVubie-4TaDKAp_Aa',
    ]
    with open('sandbox/output.json', 'w') as f:
        import json

        handler = Youtube()
        items = handler.get_playlist_items_helper(playlist_ids=playlist_ids)
        json.dump(items, f, indent=4)
