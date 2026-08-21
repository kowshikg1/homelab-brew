"""Tests for src/ddl/methods/create.py"""

import pytest

from src.ddl.methods import create as create_methods


class TestCreateConfigAndQueries:
    def test_create_config_requires_table_or_view(self):
        with pytest.raises(
            ValueError, match='Either table_name or view_name must be provided'
        ):
            create_methods.CreateConfig()

    def test_get_table_query_returns_empty_when_no_table(self):
        cfg = create_methods.CreateConfig(
            database='analytics', view_name='vw_events'
        )

        assert create_methods.get_table_query(cfg) == []

    def test_get_table_query_builds_expected_statement(self):
        cfg = create_methods.CreateConfig(
            database='analytics',
            table_name='events',
            columns=['id INTEGER PRIMARY KEY', 'name TEXT'],
        )

        query = create_methods.get_table_query(cfg)

        assert query == [
            'CREATE TABLE IF NOT EXISTS analytics.events (id INTEGER PRIMARY KEY, name TEXT);'
        ]

    def test_get_view_query_uses_explicit_view_name(self):
        cfg = create_methods.CreateConfig(
            database='analytics',
            table_name='events',
            view_name='vw_events',
            view_columns=['id', 'name'],
        )

        query = create_methods.get_view_query(cfg)

        assert query == [
            'CREATE VIEW IF NOT EXISTS analytics.vw_events AS SELECT id, name FROM analytics.events;'
        ]

    def test_get_view_query_autogenerates_for_ingestion_database(self):
        cfg = create_methods.CreateConfig(
            database='ingestion',
            table_name='events',
            columns=['id INTEGER'],
        )

        query = create_methods.get_view_query(cfg)

        assert query == [
            'CREATE VIEW IF NOT EXISTS ingestion.vw_events AS SELECT * FROM ingestion.events;'
        ]

    def test_run_returns_table_then_view_queries(self):
        queries = create_methods.run(
            {
                'database': 'ingestion',
                'table_name': 'events',
                'columns': ['id INTEGER'],
            }
        )

        assert len(queries) == 2
        assert queries[0].startswith(
            'CREATE TABLE IF NOT EXISTS ingestion.events'
        )
        assert queries[1].startswith(
            'CREATE VIEW IF NOT EXISTS ingestion.vw_events'
        )
