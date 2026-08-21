"""Tests for src/ddl/methods/__init__.py"""

from types import SimpleNamespace

import pytest

from src.ddl.ddl_utils import DDLMethod
from src.ddl.methods import get_queries


class TestGetQueries:
    def test_dispatches_create_method(self):
        cfg = SimpleNamespace(
            method=DDLMethod.CREATE,
            params={
                'database': 'ingestion',
                'table_name': 'events',
                'columns': ['id INTEGER'],
            },
        )

        queries = get_queries(cfg)

        assert len(queries) == 2
        assert queries[0].startswith(
            'CREATE TABLE IF NOT EXISTS ingestion.events'
        )
        assert queries[1].startswith(
            'CREATE VIEW IF NOT EXISTS ingestion.vw_events'
        )

    def test_raises_for_unsupported_method(self):
        cfg = SimpleNamespace(method='DROP', params={})

        with pytest.raises(ValueError, match='Unsupported method'):
            get_queries(cfg)
