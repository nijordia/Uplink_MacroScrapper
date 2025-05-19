import logging
from datetime import datetime
from typing import Dict, Any, List

from entities.economic_data import EconomicData
from interfaces.preprocessors.base_preprocessor import BasePreprocessor


class us_preprocessor(BasePreprocessor):
    """Preprocessor for US economic data."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Define metric mappings for standardization
        self.unit_mapping = {
            "gdp": "billion USD",
            "inflation": "%",
            "unemployment": "%",
            "interest_rate": "%",
            # Add more as needed
        }
        
        self.frequency_mapping = {
            "Monthly": "monthly",
            "Quarterly": "quarterly",
            "Annual": "yearly",
            # Add more as needed
        }
        
        # Define indicator mappings
        self.indicator_mapping = {
            "gdp": {
                "id": "GDP",
                "name": "Gross Domestic Product"
            },
            "inflation": {
                "id": "CPI",
                "name": "Consumer Price Index"
            },
            "unemployment": {
                "id": "UNEMP",
                "name": "Unemployment Rate"
            },
            "interest_rate": {
                "id": "INTRATE",
                "name": "Federal Funds Rate"
            }
        }
        
        # Define currency for metrics
        self.currency_mapping = {
            "gdp": "USD",
            "inflation": None,
            "unemployment": None,
            "interest_rate": None
        }
    
    def process(self, country_code: str, raw_data: Dict[str, Any]) -> List[EconomicData]:
        """Transform raw US data into standardized EconomicData instances."""
        processed_data = []
        country_name = "United States"
        
        for metric_name, data in raw_data.items():
            self.logger.info(f"Processing {metric_name} data for {country_code}")
            
            try:
                data_points = data.get("data", [])
                frequency = self.frequency_mapping.get(
                    data.get("frequency", "Monthly"),
                    "monthly"
                )
                unit = self.unit_mapping.get(metric_name, data.get("units", ""))
                
                # Get indicator info
                indicator_info = self.indicator_mapping.get(metric_name, {
                    "id": metric_name.upper(),
                    "name": metric_name.capitalize()
                })
                
                indicator_id = indicator_info["id"]
                indicator_name = indicator_info["name"]
                
                # Get currency if applicable
                currency = self.currency_mapping.get(metric_name)
                
                for point in data_points:
                    # Extract date and value
                    date_str = point.get("date")
                    value_str = point.get("value")
                    revision = point.get("revision", 0)
                    
                    if not date_str or not value_str:
                        continue
                    
                    # Parse date - format may vary
                    try:
                        if "-" in date_str:
                            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                        else:
                            date_obj = datetime.strptime(date_str, "%Y%m%d").date()
                    except ValueError:
                        self.logger.warning(f"Invalid date format: {date_str}")
                        continue
                    
                    # Parse value
                    try:
                        value = float(value_str)
                    except ValueError:
                        self.logger.warning(f"Invalid value format: {value_str}")
                        continue
                    
                    # Create EconomicData instance
                    economic_data = EconomicData(
                        country_code=country_code,
                        country_name=country_name,
                        indicator_id=indicator_id,
                        indicator_name=indicator_name,
                        value=value,
                        date=date_obj,
                        unit=unit,
                        frequency=frequency,
                        source="US Federal Reserve",
                        revision_number=revision,
                        currency=currency,
                        metadata={
                            "original_data": point,
                            "processor": "USPreprocessor"
                        }
                    )
                    
                    processed_data.append(economic_data)
                    
            except Exception as e:
                self.logger.error(f"Error processing {metric_name}: {str(e)}")
        
        return processed_data