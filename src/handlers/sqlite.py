import sqlite3
from pathlib import Path
import json
from lazy import lazy
import pandas as pd

DEFAULT_DTYPE = "TEXT"

class SQLiteHandler:
    def __init__(self, db_path: str = "data.db") -> None:
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
            print("No data to insert.")
            return
        
        self.create_table(table_name, {key: DEFAULT_DTYPE for key in data[0].keys()})
        
        columns = data[0].keys()
        placeholders = ", ".join(["?"] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        with self.connect as conn:
            cursor = conn.cursor()
            for row in data:
                values = tuple(
                    json.dumps(row.get(col), default=str) if isinstance(row.get(col), (dict, list, tuple)) else row.get(col)
                    for col in columns
                )
                cursor.execute(query, values)
            conn.commit()

    def upsert_data(self, table_name: str, data: list[dict] | pd.DataFrame, unique_key: str) -> None:
        """Upsert data into the specified table based on a unique key.
        
        :param table_name: The name of the table to upsert data into.
        :param data: A list of dictionaries or a pandas DataFrame containing the data to upsert.
        :param unique_key: The column name that serves as the unique key for conflict resolution.

        :return: None
        """

        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')
        
        if not data:
            print("No data to upsert.")
            return

        self.create_table(table_name, {key: DEFAULT_DTYPE for key in data[0].keys()}, pkey=unique_key)
        
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
                values = tuple(
                    json.dumps(row.get(col), default=str) if isinstance(row.get(col), (dict, list, tuple)) else row.get(col)
                    for col in columns
                )
                cursor.execute(query, values)
            conn.commit()

if __name__ == "__main__":
    db_handler = SQLiteHandler("ingestion.db")
    # query = "PRAGMA table_info(strava_activities);"
    query = "SELECT * FROM strava_activities;"
    result = db_handler.execute_query(query)
    print(result)