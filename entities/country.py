from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class Country:
    """Domain model representing a country and its metadata."""
    code: str  # ISO country code (e.g., 'US', 'EU')
    name: str
    region: str  # 'North America', 'Europe', etc.
    metrics_available: List[str]  # List of available metric names
    fetcher_type: str  # 'api', 'scraper', etc.
    preprocessor_type: str
    data_source_urls: Dict[str, str]  # Mapping metric names to URLs
    additional_info: Optional[Dict[str, Any]] = None
    
    def __str__(self) -> str:
        return f"{self.name} ({self.code})"