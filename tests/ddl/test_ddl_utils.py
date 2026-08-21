"""Tests for src/ddl/ddl_utils.py"""

from pathlib import Path

import pytest

from src.ddl import ddl_utils


class DummySQLiteHandler:
    def __init__(self, db_path):
        self.db_path = db_path
        self.rows = []

    def execute_query(self, query, params=None):
        return self.rows


class TestGetConfigsIter:
    def test_yields_yaml_files_in_sorted_order(self, tmp_path):
        cfg_dir = tmp_path / 'ddl'
        cfg_dir.mkdir()
        (cfg_dir / 'b.yml').write_text('name: b\n', encoding='utf-8')
        (cfg_dir / 'a.yml').write_text('name: a\n', encoding='utf-8')

        values = list(ddl_utils._get_configs_iter(cfg_dir))

        assert values == [{'name': 'a'}, {'name': 'b'}]


class TestNotExecutedConfigs:
    def test_returns_empty_when_configs_empty(self):
        assert ddl_utils._not_executed_configs([]) == []

    def test_filters_out_executed_hashes(self, monkeypatch):
        cfg_1 = {'name': 'one'}
        cfg_2 = {'name': 'two'}

        dummy = DummySQLiteHandler('ignored')

        def fake_handler(_db_path):
            return dummy

        monkeypatch.setattr(ddl_utils, 'SQLiteHandler', fake_handler)

        all_hashes = [
            ddl_utils.hash_object(cfg_1),
            ddl_utils.hash_object(cfg_2),
        ]
        dummy.rows = [(all_hashes[0],)]

        result = ddl_utils._not_executed_configs([cfg_1, cfg_2])

        assert result == [cfg_2]


class TestGetNewConfigs:
    def test_raises_for_non_positive_chunk_size(self):
        with pytest.raises(
            ValueError, match='chunk_size must be a positive integer'
        ):
            ddl_utils.get_new_configs(chunk_size=0)

    def test_collects_only_pending_configs(self, monkeypatch):
        chunk_1 = [{'id': 1}, {'id': 2}]
        chunk_2 = [{'id': 3}]

        monkeypatch.setattr(
            ddl_utils,
            '_get_configs_iter',
            lambda _config_dir: iter([chunk_1, chunk_2]),
        )

        def fake_not_executed(cfg_chunk):
            return [cfg for cfg in cfg_chunk if cfg['id'] % 2 == 1]

        monkeypatch.setattr(
            ddl_utils, '_not_executed_configs', fake_not_executed
        )

        pending = ddl_utils.get_new_configs(
            config_dir=Path('/tmp/ignored'), chunk_size=2
        )

        assert pending == [{'id': 1}, {'id': 3}]


class TestEnhanceConfigs:
    def test_enhance_with_hash_adds_hash_key(self):
        configs = [{'a': 1}, {'b': 2}]

        result = ddl_utils.enhance_configs_with_hash(configs)

        assert result is configs
        assert all('hash' in cfg for cfg in configs)

    def test_enhance_with_tag_only_fills_missing_tag(self, monkeypatch):
        monkeypatch.setattr(
            ddl_utils, 'current_timestamp', lambda precision='us': 123456
        )
        monkeypatch.setattr(ddl_utils, 'hash_object', lambda obj: 'shared-tag')

        configs = [
            {'id': 1, 'tag': 'existing'},
            {'id': 2, 'tag': None},
            {'id': 3},
        ]
        ddl_utils.enhance_configs_with_tag(configs)

        assert configs[0]['tag'] == 'existing'
        assert configs[1]['tag'] == 'shared-tag'
        assert configs[2]['tag'] == 'shared-tag'
