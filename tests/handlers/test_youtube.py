"""Tests for src/handlers/youtube.py"""

from unittest.mock import MagicMock, patch

import pytest

from src.handlers.youtube import Youtube

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def yt():
    """Return a Youtube instance with access_token and header mocked."""
    instance = Youtube.__new__(Youtube)
    instance._base_url = 'https://www.googleapis.com/youtube/v3'
    instance._token_url = 'https://oauth2.googleapis.com/token'
    instance.__dict__['access_token'] = 'fake_yt_token'
    instance.__dict__['header'] = {'Authorization': 'Bearer fake_yt_token'}
    return instance


def _make_page(items, next_token=None):
    resp = MagicMock()
    resp.status_code = 200
    data = {'items': items}
    if next_token:
        data['nextPageToken'] = next_token
    resp.json.return_value = data
    return resp


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


class TestYoutubeInit:
    def test_base_url_set(self):
        yt = Youtube.__new__(Youtube)
        yt.__init__()
        assert yt._base_url == 'https://www.googleapis.com/youtube/v3'

    def test_token_url_set(self):
        yt = Youtube.__new__(Youtube)
        yt.__init__()
        assert yt._token_url == 'https://oauth2.googleapis.com/token'


# ---------------------------------------------------------------------------
# get_playlists
# ---------------------------------------------------------------------------


class TestGetPlaylists:
    def test_raises_when_mine_and_channel_id_both_given(self, yt):
        with pytest.raises(
            ValueError, match='Cannot use channel_id and mine together'
        ):
            yt.get_playlists(channel_id='UC123', mine=True)

    def test_returns_single_page(self, yt):
        items = [{'id': 'PL1'}, {'id': 'PL2'}]
        with patch(
            'src.handlers.youtube.requests.get', return_value=_make_page(items)
        ):
            result = yt.get_playlists(channel_id='UC123')
        assert len(result) == 2

    def test_paginates_across_multiple_pages(self, yt):
        page1 = _make_page([{'id': 'PL1'}], next_token='tok1')
        page2 = _make_page([{'id': 'PL2'}, {'id': 'PL3'}])

        with patch(
            'src.handlers.youtube.requests.get', side_effect=[page1, page2]
        ) as mock_get:
            result = yt.get_playlists(channel_id='UC123')

        assert len(result) == 3
        assert mock_get.call_count == 2

    def test_raises_on_non_200(self, yt):
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = 'Forbidden'

        with patch(
            'src.handlers.youtube.requests.get', return_value=mock_response
        ):
            with pytest.raises(Exception, match='Failed to fetch playlists'):
                yt.get_playlists(channel_id='UC123')

    def test_mine_param_sent_in_request(self, yt):
        with patch(
            'src.handlers.youtube.requests.get', return_value=_make_page([])
        ) as mock_get:
            yt.get_playlists(mine=True)

        params = mock_get.call_args[1]['params']
        assert params.get('mine') is True
        assert 'channelId' not in params

    def test_channel_id_param_sent_in_request(self, yt):
        with patch(
            'src.handlers.youtube.requests.get', return_value=_make_page([])
        ) as mock_get:
            yt.get_playlists(channel_id='UC123')

        params = mock_get.call_args[1]['params']
        assert params.get('channelId') == 'UC123'


# ---------------------------------------------------------------------------
# get_playlist_ids
# ---------------------------------------------------------------------------


class TestGetPlaylistIds:
    def test_returns_ids_for_mine(self, yt):
        with patch.object(
            yt, 'get_playlists', return_value=[{'id': 'PL_mine'}]
        ):
            result = yt.get_playlist_ids(mine=True)
        assert 'PL_mine' in result

    def test_returns_ids_for_channel_ids(self, yt):
        def fake_playlists(channel_id=None, mine=False):
            return [{'id': f'PL_{channel_id}'}]

        with patch.object(yt, 'get_playlists', side_effect=fake_playlists):
            result = yt.get_playlist_ids(channel_ids=['UC1', 'UC2'])

        assert 'PL_UC1' in result
        assert 'PL_UC2' in result

    def test_deduplicates_ids(self, yt):
        with patch.object(
            yt,
            'get_playlists',
            return_value=[{'id': 'PL_dup'}, {'id': 'PL_dup'}],
        ):
            result = yt.get_playlist_ids(channel_ids=['UC1'])
        assert result.count('PL_dup') == 1

    def test_returns_empty_list_when_no_args(self, yt):
        result = yt.get_playlist_ids()
        assert result == []


# ---------------------------------------------------------------------------
# get_playlist_items
# ---------------------------------------------------------------------------


class TestGetPlaylistItems:
    def test_returns_items(self, yt):
        items = [
            {'snippet': {'title': 'Video 1'}},
            {'snippet': {'title': 'Video 2'}},
        ]
        with patch(
            'src.handlers.youtube.requests.get', return_value=_make_page(items)
        ):
            result = yt.get_playlist_items('PL1')
        assert len(result) == 2

    def test_paginates(self, yt):
        page1 = _make_page([{'snippet': {}}] * 5, next_token='tok')
        page2 = _make_page([{'snippet': {}}] * 3)

        with patch(
            'src.handlers.youtube.requests.get', side_effect=[page1, page2]
        ) as mock_get:
            result = yt.get_playlist_items('PL1')

        assert len(result) == 8
        assert mock_get.call_count == 2

    def test_raises_on_non_200(self, yt):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = 'Not Found'

        with patch(
            'src.handlers.youtube.requests.get', return_value=mock_response
        ):
            with pytest.raises(
                Exception, match='Failed to fetch playlist items'
            ):
                yt.get_playlist_items('PL_bad')

    def test_empty_playlist_returns_empty_list(self, yt):
        with patch(
            'src.handlers.youtube.requests.get', return_value=_make_page([])
        ):
            result = yt.get_playlist_items('PL_empty')
        assert result == []


# ---------------------------------------------------------------------------
# get_playlist_items_helper
# ---------------------------------------------------------------------------


class TestGetPlaylistItemsHelper:
    def test_combines_items_from_multiple_playlists(self, yt):
        def fake_items(playlist_id):
            return [{'id': f'{playlist_id}_item'}]

        with (
            patch.object(yt, 'get_playlist_ids', return_value=[]),
            patch.object(yt, 'get_playlist_items', side_effect=fake_items),
        ):
            result = yt.get_playlist_items_helper(playlist_ids=['PL1', 'PL2'])

        assert len(result) == 2

    def test_deduplicates_playlist_ids(self, yt):
        call_log = []

        def fake_items(playlist_id):
            call_log.append(playlist_id)
            return []

        with (
            patch.object(yt, 'get_playlist_ids', return_value=[]),
            patch.object(yt, 'get_playlist_items', side_effect=fake_items),
        ):
            yt.get_playlist_items_helper(playlist_ids=['PL1', 'PL1'])

        assert call_log.count('PL1') == 1

    def test_extends_with_channel_playlist_ids(self, yt):
        with (
            patch.object(
                yt, 'get_playlist_ids', return_value=['PL_from_channel']
            ),
            patch.object(
                yt, 'get_playlist_items', return_value=[{'id': 'item'}]
            ) as mock_items,
        ):
            yt.get_playlist_items_helper(channel_ids=['UC1'])

        assert mock_items.called

    def test_returns_empty_when_no_playlists(self, yt):
        with (
            patch.object(yt, 'get_playlist_ids', return_value=[]),
            patch.object(yt, 'get_playlist_items', return_value=[]),
        ):
            result = yt.get_playlist_items_helper()
        assert result == []
