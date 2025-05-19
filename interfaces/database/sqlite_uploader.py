import sqlite3
import logging
from typing import List
from entities.economic_data import EconomicData
from interfaces.database.base_uploader import BaseDataUploader

class SQLiteUploader(BaseDataUploader):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self._ensure_table()

    def _ensure_table(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS economic_data (
                pk TEXT,
                sk TEXT,
                country_code TEXT,
                country_name TEXT,
                indicator_id TEXT,
                indicator_name TEXT,
                frequency TEXT,
                date TEXT,
                value REAL,
                unit TEXT,
                source TEXT,
                revision_number INTEGER,
                currency TEXT,
                PRIMARY KEY (pk, sk)
            )
            """)

    def upload(self, data: List[EconomicData]) -> bool:
        if not data:
            return False
        with sqlite3.connect(self.db_path) as conn:
            for item in data:
                try:
                    pk = f"COUNTRY#{item.country_code}"
                    sk = f"INDICATOR#{item.indicator_id}#{item.date.isoformat()}"
                    conn.execute("""
                        INSERT OR REPLACE INTO economic_data
                        (pk, sk, country_code, country_name, indicator_id, indicator_name, frequency, date, value, unit, source, revision_number, currency)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        pk, sk, item.country_code, item.country_name, item.indicator_id, item.indicator_name,
                        item.frequency, item.date.isoformat(), item.value, item.unit, item.source,
                        item.revision_number, item.currency
                    ))
                except Exception as e:
                    self.logger.error(f"Error inserting record: {e}")
                    return False
        return True

    def check_connection(self) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"SQLite connection error: {e}")
            return False