"""Tests for src/handlers/strava.py"""

from unittest.mock import MagicMock, patch

import pytest

from src.handlers.strava import Strava

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def strava_with_token():
    """Return a Strava instance whose access_token is mocked."""
    strava = Strava.__new__(Strava)
    strava._base_url = 'https://www.strava.com/api/v3'
    strava._token_url = 'https://www.strava.com/oauth/token'
    # Bypass the lazy descriptor for access_token and header
    strava.__dict__['access_token'] = 'fake_token'
    strava.__dict__['header'] = {'Authorization': 'Bearer fake_token'}
    return strava


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


class TestStravaInit:
    def test_base_url_set(self):
        strava = Strava.__new__(Strava)
        strava.__init__()
        assert strava._base_url == 'https://www.strava.com/api/v3'

    def test_token_url_set(self):
        strava = Strava.__new__(Strava)
        strava.__init__()
        assert strava._token_url == 'https://www.strava.com/oauth/token'


# ---------------------------------------------------------------------------
# get_activities
# ---------------------------------------------------------------------------


class TestGetActivities:
    def test_returns_empty_list_when_no_activities(self, strava_with_token):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ):
            result = strava_with_token.get_activities()

        assert result == []

    def test_returns_activities(self, strava_with_token):
        activity = {
            'id': 1,
            'name': 'Morning Run',
            'start_date': '2026-01-01T07:00:00Z',
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = [
            [activity],
            [],
        ]  # page 1 has data, page 2 empty

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ):
            result = strava_with_token.get_activities()

        assert len(result) == 1
        assert result[0]['id'] == 1

    def test_paginates_until_empty_page(self, strava_with_token):
        page1 = [{'id': i} for i in range(50)]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = [page1, []]

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ) as mock_get:
            result = strava_with_token.get_activities()

        assert len(result) == 50
        assert mock_get.call_count == 2

    def test_stops_when_page_less_than_per_page(self, strava_with_token):
        partial_page = [{'id': i} for i in range(10)]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = partial_page

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ) as mock_get:
            result = strava_with_token.get_activities()

        # Only one request because page < per_page (50)
        assert mock_get.call_count == 1
        assert len(result) == 10

    def test_raises_on_non_200(self, strava_with_token):
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = 'Unauthorized'

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ):
            with pytest.raises(Exception, match='Failed to fetch activities'):
                strava_with_token.get_activities()

    def test_with_last_mtime_passes_after_param(self, strava_with_token):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ) as mock_get:
            strava_with_token.get_activities(last_mtime='2026-01-01T00:00:00Z')

        params = mock_get.call_args[1]['params']
        assert params['after'] is not None

    def test_without_last_mtime_after_is_none(self, strava_with_token):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ) as mock_get:
            strava_with_token.get_activities()

        params = mock_get.call_args[1]['params']
        assert params['after'] is None


# ---------------------------------------------------------------------------
# get_streams
# ---------------------------------------------------------------------------


class TestGetStreams:
    def test_returns_stream_for_each_activity(self, strava_with_token):
        stream_data = {
            'time': {'data': [0, 1, 2]},
            'distance': {'data': [0.0, 5.0, 10.0]},
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = stream_data

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ):
            result = strava_with_token.get_streams(
                [101, 102], keys=['time', 'distance']
            )

        assert len(result) == 2
        assert result[0]['id'] == 101
        assert result[1]['id'] == 102

    def test_stream_contains_requested_keys(self, strava_with_token):
        stream_data = {'time': {'data': [0]}, 'latlng': {'data': [[0.0, 0.0]]}}
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = stream_data

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ):
            result = strava_with_token.get_streams([1], keys=['time', 'latlng'])

        assert 'time' in result[0]
        assert 'latlng' in result[0]

    def test_raises_on_non_200(self, strava_with_token):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = 'Not Found'

        with patch(
            'src.handlers.strava.requests.get', return_value=mock_response
        ):
            with pytest.raises(Exception, match='Failed to fetch streams'):
                strava_with_token.get_streams([999], keys=['time'])

    def test_empty_activity_ids_returns_empty(self, strava_with_token):
        with patch('src.handlers.strava.requests.get') as mock_get:
            result = strava_with_token.get_streams([], keys=['time'])
        mock_get.assert_not_called()
        assert result == []


# ---------------------------------------------------------------------------
# get_streams_helper
# ---------------------------------------------------------------------------


class TestGetStreamsHelper:
    def test_returns_empty_when_no_activities(self, strava_with_token):
        with patch.object(strava_with_token, 'get_activities', return_value=[]):
            result = strava_with_token.get_streams_helper()
        assert result == []

    def test_calls_get_streams_with_activity_ids(self, strava_with_token):
        activities = [
            {'id': 1, 'start_date': '2026-01-01T07:00:00Z'},
            {'id': 2, 'start_date': '2026-01-02T07:00:00Z'},
        ]
        streams = [{'id': 1, 'time': []}, {'id': 2, 'time': []}]

        with (
            patch.object(
                strava_with_token, 'get_activities', return_value=activities
            ),
            patch.object(
                strava_with_token, 'get_streams', return_value=streams
            ) as mock_get_streams,
        ):
            strava_with_token.get_streams_helper()

        mock_get_streams.assert_called_once()
        called_ids = mock_get_streams.call_args[0][0]
        assert set(called_ids) == {1, 2}

    def test_attaches_start_date_to_streams(self, strava_with_token):
        activities = [{'id': 10, 'start_date': '2026-03-15T06:00:00Z'}]
        streams = [{'id': 10, 'time': []}]

        with (
            patch.object(
                strava_with_token, 'get_activities', return_value=activities
            ),
            patch.object(
                strava_with_token, 'get_streams', return_value=streams
            ),
        ):
            result = strava_with_token.get_streams_helper()

        assert result[0]['start_date'] == '2026-03-15T06:00:00Z'

    def test_custom_keys_passed_to_get_streams(self, strava_with_token):
        activities = [{'id': 1, 'start_date': '2026-01-01T00:00:00Z'}]
        streams = [{'id': 1}]

        with (
            patch.object(
                strava_with_token, 'get_activities', return_value=activities
            ),
            patch.object(
                strava_with_token, 'get_streams', return_value=streams
            ) as mock_gs,
        ):
            strava_with_token.get_streams_helper(keys=['time', 'heartrate'])

        assert mock_gs.call_args[1]['keys'] == ['time', 'heartrate']
