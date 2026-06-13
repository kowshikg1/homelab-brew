"""Tests for src/ingestion/ingestion_map.py"""

import pytest

from src.handlers.sqlite import SQLiteHandler
from src.handlers.strava import Strava
from src.handlers.youtube import Youtube
from src.ingestion.ingestion_map import INGESTION_MAP, get_handler_class


class TestIngestionMap:
    def test_map_contains_strava(self):
        assert 'strava' in INGESTION_MAP

    def test_map_contains_sqlite(self):
        assert 'sqlite' in INGESTION_MAP

    def test_map_contains_youtube(self):
        assert 'youtube' in INGESTION_MAP

    def test_strava_maps_to_strava_class(self):
        assert INGESTION_MAP['strava'] is Strava

    def test_sqlite_maps_to_sqlite_handler(self):
        assert INGESTION_MAP['sqlite'] is SQLiteHandler

    def test_youtube_maps_to_youtube_class(self):
        assert INGESTION_MAP['youtube'] is Youtube


class TestGetHandlerClass:
    def test_returns_strava_class(self):
        assert get_handler_class('strava') is Strava

    def test_returns_sqlite_class(self):
        assert get_handler_class('sqlite') is SQLiteHandler

    def test_returns_youtube_class(self):
        assert get_handler_class('youtube') is Youtube

    def test_unknown_handler_raises_value_error(self):
        with pytest.raises(ValueError, match="Handler 'unknown' not found"):
            get_handler_class('unknown')

    def test_case_sensitive_key(self):
        with pytest.raises(ValueError):
            get_handler_class('Strava')

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            get_handler_class('')
