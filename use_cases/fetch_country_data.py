import logging
from typing import Dict, Any, List, Type

# We only import interfaces, not concrete implementations
from interfaces.data_fetchers.base_fetcher import BaseDataFetcher


class FetchCountryDataUseCase:
    """Use case for fetching raw economic data from a country source."""
    
    def __init__(self, fetcher: BaseDataFetcher):
        self.fetcher = fetcher
        self.logger = logging.getLogger(__name__)
    
    def execute(self, country_code: str, metrics: List[str]) -> Dict[str, Any]:
        """
        Fetch raw economic data for the specified country and metrics.
        
        Args:
            country_code: ISO country code
            metrics: List of metric names to fetch
            
        Returns:
            Dictionary of raw data
        """
        self.logger.info(f"Fetching data for {country_code}, metrics: {metrics}")
        
        try:
            raw_data = self.fetcher.fetch(metrics)
            self.logger.info(f"Successfully fetched data for {country_code}")
            return raw_data
        except Exception as e:
            self.logger.error(f"Error fetching data for {country_code}: {str(e)}")
            raise