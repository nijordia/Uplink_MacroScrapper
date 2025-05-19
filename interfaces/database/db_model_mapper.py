from datetime import date
from typing import Dict, Any

from entities.economic_data import EconomicData


class DBModelMapper:
    """Maps between domain models and database representations."""
    
    def to_db_item(self, economic_data: EconomicData) -> Dict[str, Any]:
        """
        Convert EconomicData to a DynamoDB item.
        
        Args:
            economic_data: EconomicData instance
            
        Returns:
            Dictionary representation for DynamoDB
        """
        return {
            "PK": f"COUNTRY#{economic_data.country_code}",
            "SK": f"INDICATOR#{economic_data.indicator_id}#{self._format_date(economic_data.date)}",
            "country_code": economic_data.country_code,
            "country_name": economic_data.country_name,
            "indicator_id": economic_data.indicator_id,
            "indicator_name": economic_data.indicator_name,
            "value": economic_data.value,
            "date": self._format_date(economic_data.date),
            "unit": economic_data.unit,
            "frequency": economic_data.frequency,
            "source": economic_data.source,
            "year": economic_data.date.year,
            "month": economic_data.date.month,
            "day": economic_data.date.day,
            "revision_number": economic_data.revision_number,
            "currency": economic_data.currency,
            # Add metadata as flattened attributes if needed
            **(economic_data.metadata or {})
        }
    
    def from_db_item(self, item: Dict[str, Any]) -> EconomicData:
        """
        Convert a DynamoDB item to EconomicData.
        
        Args:
            item: Dictionary from DynamoDB
            
        Returns:
            EconomicData instance
        """
        # Extract date from string or components
        if "date" in item and isinstance(item["date"], str):
            date_obj = self._parse_date(item["date"])
        else:
            date_obj = date(
                year=item.get("year", 2000),
                month=item.get("month", 1),
                day=item.get("day", 1)
            )
        
        # Extract metadata (non-standard fields)
        standard_fields = {
            "PK", "SK", "country_code", "country_name", "indicator_id", "indicator_name", 
            "value", "date", "unit", "frequency", "source", "year", "month", "day",
            "revision_number", "currency"
        }
        
        metadata = {k: v for k, v in item.items() if k not in standard_fields}
        
        return EconomicData(
            country_code=item.get("country_code"),
            country_name=item.get("country_name"),
            indicator_id=item.get("indicator_id"),
            indicator_name=item.get("indicator_name"),
            value=item.get("value", 0.0),
            date=date_obj,
            unit=item.get("unit", ""),
            frequency=item.get("frequency", "monthly"),
            source=item.get("source", ""),
            revision_number=item.get("revision_number"),
            currency=item.get("currency"),
            metadata=metadata
        )
    
    def _format_date(self, date_obj: date) -> str:
        """Format date as ISO string."""
        return date_obj.isoformat()
    
    def _parse_date(self, date_str: str) -> date:
        """Parse date from ISO string."""
        return date.fromisoformat(date_str)