import logging
from datetime import datetime
from typing import Dict, Any, List
import json
import os

from entities.economic_data import EconomicData
from interfaces.preprocessors.base_preprocessor import BasePreprocessor


class cl_preprocessor(BasePreprocessor):
    """Preprocessor for Chilean economic data."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Load indicator metadata from config file
        metadata_path = os.path.join("config", "cl_indicator_metadata.json")
        with open(metadata_path, "r", encoding="utf-8") as f:
            self.indicator_metadata = json.load(f)

    def process(self, country_code: str, raw_data: Dict[str, Any]) -> List[EconomicData]:
        """Transform raw Chilean data into standardized EconomicData instances."""
        processed_data = []
        country_name = "Chile"

        for metric_name, data in raw_data.items():
            self.logger.info(f"Processing {metric_name} data for {country_code}")

            try:
                data_points = data.get("data", [])
                # Use frequency from metadata if available, else from data, else default
                frequency = self.indicator_metadata.get(metric_name, {}).get("frequency", data.get("frequency", "monthly"))
                # Use unit from metadata if available, else from data, else empty string
                unit = self.indicator_metadata.get(metric_name, {}).get("unit", data.get("units", ""))

                # Get indicator info from metadata
                indicator_info = self.indicator_metadata.get(metric_name, {
                    "id": metric_name.upper(),
                    "name": metric_name.capitalize(),
                    "currency": None
                })

                indicator_id = indicator_info.get("id", metric_name.upper())
                indicator_name = indicator_info.get("name", metric_name.capitalize())
                currency = indicator_info.get("currency", None)

                for point in data_points:
                    # Extract date and value
                    date_str = point.get("date")
                    value_str = point.get("value")

                    if not date_str or not value_str:
                        continue

                    # Parse date
                    try:
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
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
                        source="Banco Central de Chile",
                        revision_number=0,  # Chile API doesn't provide revision info
                        currency=currency,
                        metadata={
                            "original_data": point,
                            "processor": "ChilePreprocessor"
                        }
                    )

                    processed_data.append(economic_data)

            except Exception as e:
                self.logger.error(f"Error processing {metric_name}: {str(e)}")

        return processed_data