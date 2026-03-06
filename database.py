import sqlite3
import json
from datetime import datetime


class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def init(self):
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            session_id TEXT PRIMARY KEY,
            filepath TEXT,
            rows INTEGER,
            columns INTEGER,
            column_names TEXT,
            created_at TEXT
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS analyses (
            session_id TEXT PRIMARY KEY,
            results TEXT,
            created_at TEXT
        )
        """)

        conn.commit()
        conn.close()
        print("[DB] Tables initialized.")

    def save_dataset(self, session_id, filepath, rows, columns, column_names):
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
        INSERT INTO datasets VALUES (?, ?, ?, ?, ?, ?)
        """, (
            session_id,
            filepath,
            rows,
            columns,
            json.dumps(column_names),
            datetime.now().isoformat()
        ))

        conn.commit()
        conn.close()

    def get_dataset(self, session_id):
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM datasets WHERE session_id=?", (session_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                "session_id": row[0],
                "filepath": row[1],
                "rows": row[2],
                "columns": row[3],
                "column_names": json.loads(row[4])
            }
        return None

    def save_analysis(self, session_id, results):
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
        INSERT OR REPLACE INTO analyses VALUES (?, ?, ?)
        """, (
            session_id,
            json.dumps(results),
            datetime.now().isoformat()
        ))

        conn.commit()
        conn.close()