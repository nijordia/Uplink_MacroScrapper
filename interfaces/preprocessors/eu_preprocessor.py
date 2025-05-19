import logging
from datetime import datetime
from typing import Dict, Any, List

from entities.economic_data import EconomicData
from interfaces.preprocessors.base_preprocessor import BasePreprocessor


class eu_preprocessor(BasePreprocessor):
    """Preprocessor for EU economic data."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Define metric mappings for standardization
        self.unit_mapping = {
            "gdp": "billion EUR",
            "inflation": "%",
            "unemployment": "%",
            "interest_rate": "%",
            # Add more as needed
        }
        
        # Define indicator mappings
        self.indicator_mapping = {
            "gdp": {
                "id": "GDP",
                "name": "Gross Domestic Product"
            },
            "inflation": {
                "id": "HICP",
                "name": "Harmonized Index of Consumer Prices"
            },
            "unemployment": {
                "id": "UNEMP",
                "name": "Unemployment Rate"
            },
            "interest_rate": {
                "id": "EURIBOR",
                "name": "Euro Interbank Offered Rate"
            }
        }
        
        # Define currency for metrics
        self.currency_mapping = {
            "gdp": "EUR",
            "inflation": None,
            "unemployment": None,
            "interest_rate": None
        }
        
        self.column_mappings = {
            "gdp": {
                "date_column": "Period",
                "value_column": "Value"
            },
            "inflation": {
                "date_column": "Time",
                "value_column": "Rate"
            },
            # Add more as needed
        }
    
    def process(self, country_code: str, raw_data: Dict[str, Any]) -> List[EconomicData]:
        """Transform raw EU data into standardized EconomicData instances."""
        processed_data = []
        country_name = "European Union"
        
        for metric_name, data in raw_data.items():
            self.logger.info(f"Processing {metric_name} data for {country_code}")
            
            try:
                data_format = data.get("format")
                data_points = data.get("data", [])
                
                if not data_points:
                    continue
                
                # Get indicator info
                indicator_info = self.indicator_mapping.get(metric_name, {
                    "id": metric_name.upper(),
                    "name": metric_name.capitalize()
                })
                
                indicator_id = indicator_info["id"]
                indicator_name = indicator_info["name"]
                
                # Get currency if applicable
                currency = self.currency_mapping.get(metric_name)
                
                # Handle different data formats
                if data_format == "csv":
                    processed_data.extend(self._process_csv_data(
                        country_code, country_name, indicator_id,
                        indicator_name, metric_name, data_points, currency
                    ))
                elif data_format == "html_table":
                    # Flatten list of tables if needed
                    if isinstance(data_points, list) and all(isinstance(item, list) for item in data_points):
                        flattened_data = []
                        for table in data_points:
                            flattened_data.extend(table)
                        data_points = flattened_data
                        
                    processed_data.extend(self._process_table_data(
                        country_code, country_name, indicator_id, 
                        indicator_name, metric_name, data_points, currency
                    ))
                    
            except Exception as e:
                self.logger.error(f"Error processing {metric_name}: {str(e)}")
        
        return processed_data
    
    def _process_csv_data(
        self, country_code: str, country_name: str, 
        indicator_id: str, indicator_name: str, 
        metric_name: str, data_points: List[Dict[str, Any]], 
        currency: str = None
    ) -> List[EconomicData]:
        """Process data from CSV format."""
        result = []
        
        # Get column mappings for this metric
        mapping = self.column_mappings.get(metric_name, {
            "date_column": "date",
            "value_column": "value"
        })
        
        date_col = mapping["date_column"]
        value_col = mapping["value_column"]
        
        for point in data_points:
            if date_col not in point or value_col not in point:
                continue
                
            date_str = point[date_col]
            value_str = point[value_col]
            
            # Parse date - try multiple formats
            try:
                if "-" in date_str:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                elif "/" in date_str:
                    date_obj = datetime.strptime(date_str, "%d/%m/%Y").date()
                else:
                    # Try to extract year and quarter/month
                    if "Q" in date_str:  # e.g., "2023Q1"
                        year = int(date_str[:4])
                        quarter = int(date_str[5:])
                        month = (quarter - 1) * 3 + 1
                        date_obj = datetime(year, month, 1).date()
                    else:
                        date_obj = datetime.strptime(date_str, "%Y%m").date()
            except ValueError:
                self.logger.warning(f"Invalid date format: {date_str}")
                continue
            
            # Parse value
            try:
                value = float(value_str)
            except ValueError:
                # Try cleaning the string (e.g., remove commas, percentage signs)
                cleaned_value = value_str.replace(",", "").replace("%", "").strip()
                try:
                    value = float(cleaned_value)
                except ValueError:
                    self.logger.warning(f"Invalid value format: {value_str}")
                    continue
            
            # Determine frequency based on date
            frequency = "quarterly" if "Q" in date_str else "monthly"
            
            # Create EconomicData instance
            economic_data = EconomicData(
                country_code=country_code,
                country_name=country_name,
                indicator_id=indicator_id,
                indicator_name=indicator_name,
                value=value,
                date=date_obj,
                unit=self.unit_mapping.get(metric_name, ""),
                frequency=frequency,
                source="European Central Bank",
                revision_number=point.get("revision", 0),
                currency=currency,
                metadata={
                    "original_data": point,
                    "processor": "EUPreprocessor"
                }
            )
            
            result.append(economic_data)
            
        return result
    
    def _process_table_data(
        self, country_code: str, country_name: str,
        indicator_id: str, indicator_name: str,
        metric_name: str, data_points: List[Dict[str, Any]],
        currency: str = None
    ) -> List[EconomicData]:
        """Process data from HTML tables."""
        result = []
        
        for point in data_points:
            date_str = None
            value_str = None
            
            # Try to find date and value in the row
            for key, val in point.items():
                if any(keyword in key.lower() for keyword in ["date", "period", "time"]):
                    date_str = val
                elif any(keyword in key.lower() for keyword in ["value", "rate", "figure", metric_name.lower()]):
                    value_str = val
            
            if not date_str or not value_str:
                continue
                
            # Similar date and value parsing as in _process_csv_data
            try:
                # Try multiple date formats
                if "-" in date_str:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                elif "/" in date_str:
                    date_obj = datetime.strptime(date_str, "%d/%m/%Y").date()
                else:
                    # Simplified - would need enhancement for real data
                    date_obj = datetime.strptime(date_str, "%Y%m").date()
            except ValueError:
                self.logger.warning(f"Invalid date format: {date_str}")
                continue
            
            # Parse value
            try:
                value = float(value_str)
            except ValueError:
                # Try cleaning the string
                cleaned_value = value_str.replace(",", "").replace("%", "").strip()
                try:
                    value = float(cleaned_value)
                except ValueError:
                    self.logger.warning(f"Invalid value format: {value_str}")
                    continue
            
            # Create EconomicData instance with new fields
            economic_data = EconomicData(
                country_code=country_code,
                country_name=country_name,
                indicator_id=indicator_id,
                indicator_name=indicator_name,
                value=value,
                date=date_obj,
                unit=self.unit_mapping.get(metric_name, ""),
                frequency="monthly",  # Default, would need to be determined from data
                source="European Central Bank",
                revision_number=point.get("revision", 0),
                currency=currency,
                metadata={
                    "original_data": point,
                    "processor": "EUPreprocessor"
                }
            )
            
            result.append(economic_data)
            
        return result