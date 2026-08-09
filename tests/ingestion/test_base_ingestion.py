"""Tests for src/ingestion/base_ingestion.py"""

import inspect
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.base_ingestion import (
    BaseIngestion,
    ExtractMode,
    PublishMode,
    _apply_overrides,
    insert_data_to_db,
    run,
)

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TestExtractMode:
    def test_incr_value(self):
        assert ExtractMode.INCR.value == 'INCR'

    def test_hist_value(self):
        assert ExtractMode.HIST.value == 'HIST'

    def test_enum_members(self):
        assert set(e.value for e in ExtractMode) == {'INCR', 'HIST'}


class TestPublishMode:
    def test_upsert_value(self):
        assert PublishMode.UPSERT.value == 'UPSERT'

    def test_append_value(self):
        assert PublishMode.APPEND.value == 'APPEND'

    def test_truncate_value(self):
        assert PublishMode.TRUNCATE.value == 'TRUNCATE'

    def test_enum_members(self):
        assert set(e.value for e in PublishMode) == {
            'UPSERT',
            'APPEND',
            'TRUNCATE',
        }


# ---------------------------------------------------------------------------
# BaseIngestion
# ---------------------------------------------------------------------------


# TODO: Use generic tests instead of using strava
@pytest.fixture
def mock_strava_class():
    """A mock handler class that can be instantiated."""
    mock_instance = MagicMock()
    mock_class = MagicMock(return_value=mock_instance)
    return mock_class, mock_instance


class TestBaseIngestion:
    def _make_job(self, overrides=None, mock_handler_class=None):
        defaults = dict(
            job_name='my_job',
            handler='strava',
            extract_method='get_activities',
            table='activities',
        )
        if overrides:
            defaults.update(overrides)

        if mock_handler_class is None:
            mock_handler_class = MagicMock()

        with patch(
            'src.ingestion.base_ingestion.get_handler_class',
            return_value=mock_handler_class,
        ):
            job = BaseIngestion(**defaults)
        return job, mock_handler_class

    def test_handler_class_set_from_handler_name(self):
        mock_cls = MagicMock()
        with patch(
            'src.ingestion.base_ingestion.get_handler_class',
            return_value=mock_cls,
        ) as mock_get:
            job = BaseIngestion(
                job_name='my_job',
                handler='strava',
                extract_method='get_activities',
                table='t',
            )
        mock_get.assert_called_once_with('strava')
        assert job.handler_class is mock_cls

    def test_handler_instance_created(self):
        mock_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_instance)
        with patch(
            'src.ingestion.base_ingestion.get_handler_class',
            return_value=mock_cls,
        ):
            job = BaseIngestion(
                job_name='my_job',
                handler='strava',
                extract_method='get_activities',
                table='t',
            )
        assert job.handler_instance is mock_instance

    def test_extract_init_passed_to_handler_class(self):
        mock_cls = MagicMock()
        with patch(
            'src.ingestion.base_ingestion.get_handler_class',
            return_value=mock_cls,
        ):
            BaseIngestion(
                job_name='my_job',
                handler='strava',
                extract_method='run',
                table='t',
                extract_init={'db_path': ':memory:'},
            )
        mock_cls.assert_called_once_with(db_path=':memory:')

    def test_custom_handler_class_skips_get_handler_class(self):
        mock_cls = MagicMock()
        with patch(
            'src.ingestion.base_ingestion.get_handler_class'
        ) as mock_get:
            BaseIngestion(
                job_name='my_job',
                handler='strava',
                extract_method='run',
                table='t',
                handler_class=mock_cls,
            )
        mock_get.assert_not_called()

    def test_default_extract_mode_is_incr(self):
        job, _ = self._make_job()
        assert job.extract_mode == ExtractMode.INCR.value

    def test_default_publish_mode_is_upsert(self):
        job, _ = self._make_job()
        assert job.publish_mode == PublishMode.UPSERT.value

    def test_default_is_active_true(self):
        job, _ = self._make_job()
        assert job.is_active is True

    def test_default_send_notification_false(self):
        job, _ = self._make_job()
        assert job.send_notification is False

    def test_overrides_applied(self):
        job, _ = self._make_job(
            {'table': 'custom_table', 'database': 'custom.db'}
        )
        assert job.table == 'custom_table'
        assert job.database == 'custom.db'


# ---------------------------------------------------------------------------
# insert_data_to_db
# ---------------------------------------------------------------------------


class TestInsertDataToDB:
    def _make_job(
        self,
        publish_mode,
        extract_mode=ExtractMode.INCR.value,
        id_config_col='id',
    ):
        mock_cls = MagicMock()
        with patch(
            'src.ingestion.base_ingestion.get_handler_class',
            return_value=mock_cls,
        ):
            job = BaseIngestion(
                job_name='my_job',
                handler='strava',
                extract_method='run',
                table='test_table',
                publish_mode=publish_mode,
                extract_mode=extract_mode,
                id_config_col=id_config_col,
            )
        return job

    def test_upsert_mode_calls_upsert_data(self):
        job = self._make_job(PublishMode.UPSERT.value)
        mock_sqlite = MagicMock()
        data = [{'id': 1}]

        with patch(
            'src.ingestion.base_ingestion.SQLiteHandler',
            return_value=mock_sqlite,
        ):
            insert_data_to_db(job, data)

        mock_sqlite.upsert_data.assert_called_once_with(
            table_name='test_table', data=data, unique_key='id'
        )

    def test_truncate_mode_truncates_then_inserts(self):
        job = self._make_job(
            PublishMode.TRUNCATE.value, extract_mode=ExtractMode.HIST.value
        )
        mock_sqlite = MagicMock()
        data = [{'id': 1}]

        with patch(
            'src.ingestion.base_ingestion.SQLiteHandler',
            return_value=mock_sqlite,
        ):
            insert_data_to_db(job, data)

        mock_sqlite.truncate_table.assert_called_once_with('test_table')

    def test_append_mode_calls_insert_data(self):
        job = self._make_job(PublishMode.APPEND.value)
        mock_sqlite = MagicMock()
        data = [{'id': 1}]

        with patch(
            'src.ingestion.base_ingestion.SQLiteHandler',
            return_value=mock_sqlite,
        ):
            insert_data_to_db(job, data)

        mock_sqlite.insert_data.assert_called_once_with(
            table_name='test_table', data=data
        )

    def test_hist_mode_append_calls_insert_data(self):
        job = self._make_job(
            PublishMode.APPEND.value, extract_mode=ExtractMode.HIST.value
        )
        mock_sqlite = MagicMock()
        data = [{'id': 2}]

        with patch(
            'src.ingestion.base_ingestion.SQLiteHandler',
            return_value=mock_sqlite,
        ):
            insert_data_to_db(job, data)

        mock_sqlite.insert_data.assert_called_once()

    def test_invalid_combination_raises_value_error(self):
        job = self._make_job(publish_mode='INVALID_MODE')
        mock_sqlite = MagicMock()

        with patch(
            'src.ingestion.base_ingestion.SQLiteHandler',
            return_value=mock_sqlite,
        ):
            with pytest.raises(ValueError, match='Unsupported publish mode'):
                insert_data_to_db(job, [{'id': 1}])


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


class TestRun:
    def _config(self, extra=None):
        config = {
            'job_name': 'my_job',
            'handler': 'strava',
            'extract_method': 'get_activities',
            'table': 'activities',
            'id_config_col': 'id',
            'watermark_col': 'start_date',
        }
        if extra:
            config.update(extra)
        return config

    def _raw_run(self):
        # Always execute the undecorated function body in unit tests.
        return inspect.unwrap(run)

    def test_run_calls_extract_method(self):
        mock_instance = MagicMock()
        mock_instance.get_activities.return_value = [{'id': 1}]
        mock_cls = MagicMock(return_value=mock_instance)
        raw_run = self._raw_run()

        with (
            patch(
                'src.ingestion.base_ingestion.get_handler_class',
                return_value=mock_cls,
            ),
            patch(
                'src.ingestion.base_ingestion.SQLiteHandler'
            ) as mock_sqlite_cls,
        ):
            mock_sqlite_cls.return_value.get_last_mtime.return_value = None
            mock_sqlite_cls.return_value.upsert_data.return_value = None
            raw_run(self._config())

        mock_instance.get_activities.assert_called_once()

    def test_run_fetches_last_mtime_for_incr_mode(self):
        mock_instance = MagicMock()
        mock_instance.get_activities.return_value = []
        mock_cls = MagicMock(return_value=mock_instance)

        mock_sqlite = MagicMock()
        mock_sqlite.get_last_mtime.return_value = '2024-01-01'
        mock_sqlite.upsert_data.return_value = None
        raw_run = self._raw_run()

        with (
            patch(
                'src.ingestion.base_ingestion.get_handler_class',
                return_value=mock_cls,
            ),
            patch(
                'src.ingestion.base_ingestion.SQLiteHandler',
                return_value=mock_sqlite,
            ),
        ):
            raw_run(self._config())

        mock_sqlite.get_last_mtime.assert_called_once()

    def test_run_calls_insert_data_to_db(self):
        mock_instance = MagicMock()
        mock_instance.get_activities.return_value = [
            {'id': 1, 'start_date': '2024-01-01'}
        ]
        mock_cls = MagicMock(return_value=mock_instance)

        mock_sqlite = MagicMock()
        mock_sqlite.get_last_mtime.return_value = None
        raw_run = self._raw_run()

        with (
            patch(
                'src.ingestion.base_ingestion.get_handler_class',
                return_value=mock_cls,
            ),
            patch(
                'src.ingestion.base_ingestion.SQLiteHandler',
                return_value=mock_sqlite,
            ),
        ):
            raw_run(self._config())

        mock_sqlite.upsert_data.assert_called_once()


class TestApplyOverrides:
    def test_applies_supported_overrides(self):
        config = {
            'job_name': 'my_job',
            'handler': 'strava',
            'extract_method': 'run',
            'table': 'activities',
            'extract_params': {},
        }
        updated = _apply_overrides(
            config,
            (
                'publish_mode="APPEND"',
                'send_notification=true',
                'extract_params={"limit":10}',
            ),
        )

        assert updated['publish_mode'] == 'APPEND'
        assert updated['send_notification'] is True
        assert updated['extract_params'] == {'limit': 10}

    def test_rejects_invalid_override_format(self):
        config = {
            'job_name': 'my_job',
            'handler': 'strava',
            'extract_method': 'run',
            'table': 'activities',
        }
        with pytest.raises(Exception, match='key=value'):
            _apply_overrides(config, ('invalid_override',))
