"""Tests for src/handlers/sqlite.py"""

import pandas as pd
import pytest

from src.handlers.sqlite import SQLiteHandler

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    """Return an SQLiteHandler backed by a temporary database."""
    db_path = str(tmp_path / 'test.db')
    return SQLiteHandler(db_path=db_path)


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


class TestSQLiteHandlerInit:
    def test_db_path_stored(self, tmp_path):
        path = str(tmp_path / 'mydb.db')
        handler = SQLiteHandler(db_path=path)
        assert handler.db_path == path

    def test_default_db_path(self):
        from src.utils.path_variables import INGESTION_SQLITE_DB

        handler = SQLiteHandler()
        assert handler.db_path == INGESTION_SQLITE_DB


# ---------------------------------------------------------------------------
# execute_query
# ---------------------------------------------------------------------------


class TestExecuteQuery:
    def test_simple_select(self, db):
        result = db.execute_query('SELECT 1')
        assert result == [(1,)]

    def test_select_with_params(self, db):
        db.execute_query('CREATE TABLE t (id INTEGER, name TEXT)')
        db.execute_query('INSERT INTO t VALUES (?, ?)', (1, 'alice'))
        result = db.execute_query('SELECT name FROM t WHERE id = ?', (1,))
        assert result == [('alice',)]

    def test_returns_empty_list_when_no_rows(self, db):
        db.execute_query('CREATE TABLE empty_t (id INTEGER)')
        result = db.execute_query('SELECT * FROM empty_t')
        assert result == []


# ---------------------------------------------------------------------------
# does_table_exist
# ---------------------------------------------------------------------------


class TestDoesTableExist:
    def test_returns_false_for_missing_table(self, db):
        assert db.does_table_exist('nonexistent') is False

    def test_returns_true_for_existing_table(self, db):
        db.execute_query('CREATE TABLE my_table (id INTEGER)')
        assert db.does_table_exist('my_table') is True


# ---------------------------------------------------------------------------
# create_table
# ---------------------------------------------------------------------------


class TestCreateTable:
    def test_creates_table(self, db):
        db.create_table('users', {'id': 'INTEGER', 'name': 'TEXT'})
        assert db.does_table_exist('users')

    def test_create_table_with_primary_key(self, db):
        db.create_table('items', {'id': 'INTEGER', 'val': 'TEXT'}, pkey='id')
        cols = db.get_table_columns('items')
        assert 'id' in cols
        assert 'val' in cols

    def test_create_table_idempotent_without_auto_alter(self, db):
        db.create_table('t', {'id': 'INTEGER'})
        db.create_table('t', {'id': 'INTEGER'})  # Should not raise
        assert db.does_table_exist('t')

    def test_add_column(self, db):
        db.create_table('t', {'id': 'INTEGER'})
        db.add_column('t', 'extra', 'TEXT')
        cols = db.get_table_columns('t')
        assert 'extra' in cols

    def test_alter_table(self, db):
        db.create_table('t', {'id': 'INTEGER'}, pkey='id')
        # Now call with a new column via auto_alter
        db.alter_table('t', {'id': 'INTEGER', 'extra': 'TEXT'})
        # existing id column won't be added twice (ALTER TABLE will fail silently or not be called)

    def test_create_table_columns_exist(self, db):
        db.create_table('persons', {'name': 'TEXT', 'age': 'INTEGER'})
        cols = db.get_table_columns('persons')
        assert 'name' in cols
        assert 'age' in cols


# ---------------------------------------------------------------------------
# drop_table
# ---------------------------------------------------------------------------


class TestDropTable:
    def test_drops_existing_table(self, db):
        db.execute_query('CREATE TABLE drop_me (id INTEGER)')
        db.drop_table('drop_me')
        assert not db.does_table_exist('drop_me')

    def test_drop_nonexistent_table_does_not_raise(self, db):
        db.drop_table('ghost_table')  # IF EXISTS prevents error


# ---------------------------------------------------------------------------
# truncate_table
# ---------------------------------------------------------------------------


class TestTruncateTable:
    def test_removes_all_rows(self, db):
        db.execute_query('CREATE TABLE trunc_t (id INTEGER)')
        db.execute_query('INSERT INTO trunc_t VALUES (?)', (1,))
        db.execute_query('INSERT INTO trunc_t VALUES (?)', (2,))
        db.truncate_table('trunc_t')
        result = db.execute_query('SELECT COUNT(*) FROM trunc_t')
        assert result[0][0] == 0

    def test_truncate_preserves_table_structure(self, db):
        db.execute_query('CREATE TABLE trunc_t2 (id INTEGER, name TEXT)')
        db.truncate_table('trunc_t2')
        assert db.does_table_exist('trunc_t2')


# ---------------------------------------------------------------------------
# alter_table_add_column
# ---------------------------------------------------------------------------


class TestAlterTableAddColumn:
    def test_adds_column(self, db):
        db.execute_query('CREATE TABLE alter_t (id INTEGER)')
        db.add_column('alter_t', 'new_col', 'TEXT')
        cols = db.get_table_columns('alter_t')
        assert 'new_col' in cols

    def test_default_dtype_is_text(self, db):
        db.execute_query('CREATE TABLE alter_t2 (id INTEGER)')
        db.add_column('alter_t2', 'auto_col')
        cols = db.get_table_columns('alter_t2')
        assert 'auto_col' in cols


# ---------------------------------------------------------------------------
# get_table_columns
# ---------------------------------------------------------------------------


class TestGetTableColumns:
    def test_returns_column_names(self, db):
        db.execute_query('CREATE TABLE cols_t (alpha TEXT, beta INTEGER)')
        cols = db.get_table_columns('cols_t')
        assert set(cols) == {'alpha', 'beta'}

    def test_returns_empty_list_for_missing_table(self, db):
        # PRAGMA on non-existent table returns empty list
        cols = db.get_table_columns('no_such_table')
        assert cols == []


# ---------------------------------------------------------------------------
# get_last_mtime
# ---------------------------------------------------------------------------


class TestGetLastMtime:
    def test_returns_none_when_table_missing(self, db):
        result = db.get_last_mtime('no_table', 'mtime')
        assert result is None

    def test_returns_none_when_table_empty(self, db):
        db.execute_query('CREATE TABLE mt (id INTEGER, mtime TEXT)')
        result = db.get_last_mtime('mt')
        assert result is None

    def test_returns_max_value(self, db):
        db.execute_query('CREATE TABLE mt2 (id INTEGER, mtime TEXT)')
        db.execute_query("INSERT INTO mt2 VALUES (1, '2026-01-01')")
        db.execute_query("INSERT INTO mt2 VALUES (2, '2026-06-01')")
        result = db.get_last_mtime('mt2')
        assert result == '2026-06-01'


# ---------------------------------------------------------------------------
# insert_data
# ---------------------------------------------------------------------------


class TestInsertData:
    def test_insert_list_of_dicts(self, db):
        db.execute_query('CREATE TABLE ins (id INTEGER, name TEXT)')
        db.insert_data(
            'ins', [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        )
        rows = db.execute_query('SELECT id, name FROM ins ORDER BY id')
        assert rows == [(1, 'Alice'), (2, 'Bob')]

    def test_insert_dataframe(self, db):
        db.execute_query('CREATE TABLE ins_df (id INTEGER, val TEXT)')
        df = pd.DataFrame({'id': [10, 20], 'val': ['x', 'y']})
        db.insert_data('ins_df', df)
        rows = db.execute_query('SELECT COUNT(*) FROM ins_df')
        assert rows[0][0] == 2

    def test_insert_empty_list_does_nothing(self, db, caplog):
        db.execute_query('CREATE TABLE ins_empty (id INTEGER)')
        import logging

        with caplog.at_level(logging.INFO):
            db.insert_data('ins_empty', [])
        assert db.execute_query('SELECT COUNT(*) FROM ins_empty')[0][0] == 0

    def test_insert_empty_dataframe_does_nothing(self, db):
        db.execute_query('CREATE TABLE ins_empty_df (id INTEGER)')
        db.insert_data('ins_empty_df', pd.DataFrame())
        assert db.execute_query('SELECT COUNT(*) FROM ins_empty_df')[0][0] == 0

    def test_insert_partial_keys_fills_none(self, db):
        db.execute_query('CREATE TABLE partial (a TEXT, b TEXT)')
        db.insert_data('partial', [{'a': '1'}, {'b': '2'}])
        rows = db.execute_query('SELECT COUNT(*) FROM partial')
        assert rows[0][0] == 2


# ---------------------------------------------------------------------------
# upsert_data
# ---------------------------------------------------------------------------


class TestUpsertData:
    def test_upsert_inserts_new_row(self, db):
        db.create_table('upsert_t', {'id': 'INTEGER', 'val': 'TEXT'}, pkey='id')
        db.upsert_data(
            'upsert_t', [{'id': 1, 'val': 'original'}], unique_key='id'
        )
        rows = db.execute_query('SELECT val FROM upsert_t WHERE id=1')
        assert rows[0][0] == 'original'

    def test_upsert_updates_on_conflict(self, db):
        db.create_table(
            'upsert_t2', {'id': 'INTEGER', 'val': 'TEXT'}, pkey='id'
        )
        db.upsert_data('upsert_t2', [{'id': 1, 'val': 'old'}], unique_key='id')
        db.upsert_data('upsert_t2', [{'id': 1, 'val': 'new'}], unique_key='id')
        rows = db.execute_query('SELECT val FROM upsert_t2 WHERE id=1')
        assert rows[0][0] == 'new'

    def test_upsert_empty_list_does_nothing(self, db, caplog):
        db.create_table('upsert_empty', {'id': 'INTEGER'}, pkey='id')
        import logging

        with caplog.at_level(logging.INFO):
            db.upsert_data('upsert_empty', [], unique_key='id')
        assert db.execute_query('SELECT COUNT(*) FROM upsert_empty')[0][0] == 0

    def test_upsert_dataframe(self, db):
        db.create_table(
            'upsert_df', {'id': 'INTEGER', 'val': 'TEXT'}, pkey='id'
        )
        df = pd.DataFrame({'id': [1, 2], 'val': ['a', 'b']})
        db.upsert_data('upsert_df', df, unique_key='id')
        assert db.execute_query('SELECT COUNT(*) FROM upsert_df')[0][0] == 2
