import sqlite3
from pathlib import Path
from lazy import lazy
import pandas as pd

from src.utils.log_util import get_logger
from src.utils.path_variables import DEFAULT_SQLITE_DB
from src.utils.commons import to_text

DEFAULT_DTYPE = "TEXT"
log = get_logger(Path(__file__).stem)

class SQLiteHandler:
    def __init__(self, db_path: str = DEFAULT_SQLITE_DB) -> None:
        self.db_path = db_path

    @lazy
    def connect(self):
        return sqlite3.connect(self.db_path)

    def execute_query(self, query: str, params=None):
        with self.connect as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    def create_table(self, table_name: str, columns: dict, pkey: str = None, auto_alter: bool = False,) -> None:
        """
        :param pkey: The column name to set as PRIMARY KEY (optional).
        :param auto_alter: If True, automatically add new columns if they don't exist (default: False).
        """
        if self.does_table_exist(table_name):
            if auto_alter:
                self.alter_table_add_column(table_name, pkey, columns[pkey],) if pkey else None
            return
        if pkey and pkey in columns:
            columns_def = ", ".join([f"{col} {dtype}{' PRIMARY KEY' if col == pkey else ''}" for col, dtype in columns.items()])
        else:
            columns_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"
        self.execute_query(query)
        if auto_alter:
            existing_columns = self.get_table_columns(table_name)
            for col, dtype in columns.items():
                if col not in existing_columns:
                    self.alter_table_add_column(table_name, col, dtype)
    
    def does_table_exist(self, table_name: str) -> bool:
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        result = self.execute_query(query)
        return len(result) > 0
    
    def drop_table(self, table_name: str) -> None:
        query = f"DROP TABLE IF EXISTS {table_name}"
        self.execute_query(query)
    
    def truncate_table(self, table_name: str) -> None:
        query = f"DELETE FROM {table_name}"
        self.execute_query(query)
    
    def alter_table_add_column(self, table_name: str, column_name: str, dtype: str = DEFAULT_DTYPE) -> None:
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {dtype}"
        self.execute_query(query)
    
    def get_table_columns(self, table_name: str) -> list:
        query = f"PRAGMA table_info({table_name})"
        result = self.execute_query(query)
        return [row[1] for row in result] if result else []

    def get_last_mtime(self, table_name: str, watermark_col: str = "mtime") -> float:
        if not self.does_table_exist(table_name):
            return None
        query = f"SELECT MAX({watermark_col}) FROM {table_name}"
        result = self.execute_query(query)
        return result[0][0] if result and result[0][0] else None
    
    def insert_data(self, table_name: str, data: list[dict] | pd.DataFrame) -> None:
        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')
        
        if not data:
            log.info("No data to insert.")
            return

        columns = data[0].keys()
        placeholders = ", ".join(["?"] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        with self.connect as conn:
            cursor = conn.cursor()
            for row in data:
                values = tuple(to_text(row.get(col)) for col in columns)
                cursor.execute(query, values)
            conn.commit()

    def upsert_data(self, table_name: str, data: list[dict] | pd.DataFrame, unique_key: str) -> None:
        """Upsert data into the specified table based on a unique key.

        :param unique_key: The column name that serves as the unique key for conflict resolution.
        """

        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')
        
        if not data:
            log.info("No data to upsert.")
            return
        
        columns = data[0].keys()
        placeholders = ", ".join(["?"] * len(columns))
        update_placeholders = ", ".join([f"{col}=excluded.{col}" for col in columns if col != unique_key])
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES ({placeholders})
            ON CONFLICT({unique_key}) DO UPDATE SET {update_placeholders}
        """
        
        with self.connect as conn:
            cursor = conn.cursor()
            for row in data:
                values = tuple(to_text(row.get(col)) for col in columns)
                cursor.execute(query, values)
            conn.commit()

if __name__ == "__main__":
    db_handler = SQLiteHandler("./data/ingestion.db")
    # query = "PRAGMA table_info(strava_activities);"
    query = "SELECT * FROM strava_activities;"
    result = db_handler.execute_query(query)
    print(result)