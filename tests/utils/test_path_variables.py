"""Tests for src/utils/path_variables.py"""

from pathlib import Path

from src.utils.path_variables import (
    DEFAULT_SQLITE_DB,
    ENV_FILE_GLOBAL,
    ENV_FILE_HANDLERS,
    INGESTION_SQLITE_DB,
    PATH_COMPILED_SERVICES_CATALOG,
    PATH_DDL_FOLDER,
    PATH_INGESTION_CONFIG,
    PATH_INGESTION_FOLDER,
    PATH_SERVICES_CATALOG,
    SQLITE_DB_FOLDER,
)


class TestPathVariables:
    def test_env_file_global_is_path(self):
        assert isinstance(ENV_FILE_GLOBAL, Path)

    def test_env_file_global_name(self):
        assert ENV_FILE_GLOBAL.name == '.env'

    def test_env_file_handlers_is_path(self):
        assert isinstance(ENV_FILE_HANDLERS, Path)

    def test_env_file_handlers_name(self):
        assert ENV_FILE_HANDLERS.name == '.env'

    def test_env_file_handlers_in_src_handlers(self):
        assert 'handlers' in str(ENV_FILE_HANDLERS)

    def test_path_ingestion_config_is_path(self):
        assert isinstance(PATH_INGESTION_CONFIG, Path)

    def test_path_ingestion_config_suffix(self):
        assert PATH_INGESTION_CONFIG.suffix == '.json'

    def test_path_ingestion_folder_is_path(self):
        assert isinstance(PATH_INGESTION_FOLDER, Path)

    def test_path_ingestion_folder_name(self):
        assert PATH_INGESTION_FOLDER.name == 'ingestion'

    def test_path_ddl_folder_is_path(self):
        assert isinstance(PATH_DDL_FOLDER, Path)

    def test_path_ddl_folder_name(self):
        assert PATH_DDL_FOLDER.name == 'ddl'

    def test_path_services_catalog_is_path(self):
        assert isinstance(PATH_SERVICES_CATALOG, Path)

    def test_path_services_catalog_suffix(self):
        assert PATH_SERVICES_CATALOG.suffix == '.yml'

    def test_path_compiled_services_catalog_is_path(self):
        assert isinstance(PATH_COMPILED_SERVICES_CATALOG, Path)

    def test_path_compiled_services_catalog_suffix(self):
        assert PATH_COMPILED_SERVICES_CATALOG.suffix == '.json'

    def test_sqlite_db_folder_is_path(self):
        assert isinstance(SQLITE_DB_FOLDER, Path)

    def test_sqlite_db_folder_name(self):
        assert SQLITE_DB_FOLDER.name == 'data'

    def test_default_sqlite_db_is_string(self):
        assert isinstance(DEFAULT_SQLITE_DB, str)

    def test_default_sqlite_db_ends_with_db(self):
        assert DEFAULT_SQLITE_DB.endswith('.db')

    def test_ingestion_sqlite_db_is_string(self):
        assert isinstance(INGESTION_SQLITE_DB, str)

    def test_ingestion_sqlite_db_ends_with_db(self):
        assert INGESTION_SQLITE_DB.endswith('.db')

    def test_default_and_ingestion_db_are_different(self):
        assert DEFAULT_SQLITE_DB != INGESTION_SQLITE_DB
