from dataclasses import dataclass
from datetime import date
from typing import Optional, Any


@dataclass
class EconomicData:
    """Core domain model representing economic data."""
    country_code: str
    country_name: str
    indicator_id: str
    indicator_name: str
    value: float
    date: date
    frequency: str  # 'monthly', 'quarterly', 'yearly'
    unit: str
    source: str
    revision_number: Optional[int] = None
    currency: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None
    
    def __str__(self) -> str:
        return f"{self.country_name} ({self.country_code}) - {self.indicator_name}: {self.value} {self.unit} ({self.date})"