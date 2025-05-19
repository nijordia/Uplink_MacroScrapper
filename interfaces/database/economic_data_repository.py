import sqlite3
from typing import List
from entities.economic_data import EconomicData


class EconomicDataRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_latest_by_indicator_name(self, country_code: str, keyword: str) -> List[EconomicData]:
        query = """
            SELECT * FROM economic_data
            WHERE country_code = ? AND LOWER(indicator_name) LIKE ?
            ORDER BY indicator_id, date DESC
        """
        results = {}
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(query, (country_code, f"%{keyword.lower()}%")):
                indicator_id = row["indicator_id"]
                if indicator_id not in results:
                    results[indicator_id] = EconomicData(
                        country_code=row["country_code"],
                        country_name=row["country_name"],
                        indicator_id=row["indicator_id"],
                        indicator_name=row["indicator_name"],
                        value=row["value"],
                        date=row["date"],
                        frequency=row["frequency"],
                        unit=row["unit"],
                        source=row["source"],
                        revision_number=row["revision_number"],
                        currency=row["currency"],
                        metadata=None
                    )
        # Return latest entry per indicator, sorted by value descending
        return sorted(results.values(), key=lambda x: x.value, reverse=True)

