"""Tests for src/ddl/base_ddl.py"""

import sqlite3

import pytest

from src.ddl.base_ddl import BaseDDLConfig, run_configs
from src.ddl.ddl_utils import DDLMethod


class FakeConnection:
    def __init__(self):
        self.execute_calls = []
        self.executemany_calls = []
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query):
        self.execute_calls.append(query)

    def executemany(self, query, rows):
        self.executemany_calls.append((query, rows))

    def commit(self):
        self.committed = True


class TestBaseDDLConfig:
    def test_method_is_coerced_to_enum(self):
        cfg = BaseDDLConfig(method='CREATE', tag='t1', params={}, hash='h1')

        assert cfg.method == DDLMethod.CREATE


class TestRunConfigs:
    def test_executes_attach_queries_and_writes_audit_rows(self, monkeypatch):
        from src.ddl import base_ddl

        config_a = BaseDDLConfig(
            method=DDLMethod.CREATE,
            tag='t1',
            params={'database': 'db_a'},
            hash='h_a',
            order=2,
        )
        config_b = BaseDDLConfig(
            method=DDLMethod.CREATE,
            tag='t1',
            params={'database': 'db_b'},
            hash='h_b',
            order=1,
        )

        original_map = {
            'h_a': {
                'method': 'CREATE',
                'tag': 't1',
                'order': 2,
                'params': {'database': 'db_a'},
            },
            'h_b': {
                'method': 'CREATE',
                'tag': 't1',
                'order': 1,
                'params': {'database': 'db_b'},
            },
        }

        fake_conn = FakeConnection()

        monkeypatch.setattr(base_ddl.sqlite3, 'connect', lambda _db: fake_conn)
        monkeypatch.setattr(base_ddl, 'get_git_head', lambda: 'deadbeef')
        monkeypatch.setattr(
            base_ddl,
            'get_queries',
            lambda cfg: [f'-- query for {cfg.hash}'],
        )

        executed_count = run_configs(
            [config_a, config_b], original_config_map=original_map
        )

        assert executed_count == 2
        assert fake_conn.committed is True
        assert any(
            'ATTACH DATABASE' in q and 'db_a' in q
            for q in fake_conn.execute_calls
        )
        assert any(
            'ATTACH DATABASE' in q and 'db_b' in q
            for q in fake_conn.execute_calls
        )
        assert '-- query for h_b' in fake_conn.execute_calls
        assert '-- query for h_a' in fake_conn.execute_calls
        assert len(fake_conn.executemany_calls) == 1
        insert_query, rows = fake_conn.executemany_calls[0]
        assert 'INSERT INTO ingest_ddl' in insert_query
        assert rows == [
            ('h_b', 'CREATE', 't1', 1, "{'database': 'db_b'}", 'deadbeef'),
            ('h_a', 'CREATE', 't1', 2, "{'database': 'db_a'}", 'deadbeef'),
        ]

    def test_re_raises_sqlite_error(self, monkeypatch):
        from src.ddl import base_ddl

        def raise_sqlite_error(_db):
            raise sqlite3.Error('boom')

        monkeypatch.setattr(base_ddl.sqlite3, 'connect', raise_sqlite_error)
        monkeypatch.setattr(base_ddl, 'get_queries', lambda _cfg: ['SELECT 1;'])

        cfg = BaseDDLConfig(
            method=DDLMethod.CREATE,
            tag='t1',
            params={'database': 'db_a'},
            hash='h_a',
            order=1,
        )

        with pytest.raises(sqlite3.Error, match='boom'):
            run_configs(
                [cfg],
                original_config_map={
                    'h_a': {'method': 'CREATE', 'params': {'database': 'db_a'}}
                },
            )


class TestMain:
    def test_returns_zero_when_no_new_configs(self, monkeypatch):
        from src.ddl import base_ddl

        monkeypatch.setattr(base_ddl, 'get_new_configs', lambda: [])

        total = base_ddl.main()

        assert total == 0

    def test_groups_by_tag_and_sums_results(self, monkeypatch):
        from src.ddl import base_ddl

        configs = [
            {
                'method': 'CREATE',
                'tag': 'tag-a',
                'params': {'database': 'ingestion'},
            },
            {
                'method': 'CREATE',
                'tag': 'tag-a',
                'params': {'database': 'analytics'},
            },
            {
                'method': 'CREATE',
                'tag': 'tag-b',
                'params': {'database': 'ingestion'},
            },
        ]

        def fake_enhance_hash(cfgs):
            for idx, cfg in enumerate(cfgs, start=1):
                cfg['hash'] = f'h-{idx}'
            return cfgs

        monkeypatch.setattr(base_ddl, 'get_new_configs', lambda: configs)
        monkeypatch.setattr(
            base_ddl, 'enhance_configs_with_hash', fake_enhance_hash
        )
        monkeypatch.setattr(
            base_ddl, 'enhance_configs_with_tag', lambda cfgs: cfgs
        )

        calls = []

        def fake_run_configs(group_cfgs, original_config_map=None):
            calls.append((group_cfgs, original_config_map))
            return len(group_cfgs)

        monkeypatch.setattr(base_ddl, 'run_configs', fake_run_configs)

        total = base_ddl.main()

        assert total == 3
        assert len(calls) == 2
        assert sorted(len(group) for group, _map in calls) == [1, 2]
