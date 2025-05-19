import os
import logging
import requests
from typing import Dict, Any, List
from dotenv import load_dotenv

from interfaces.data_fetchers.base_fetcher import BaseDataFetcher


class us_api_fetcher(BaseDataFetcher):
    """Implementation for fetching US economic data from APIs."""
    
    def __init__(self, api_key: str = None, base_url: str = None):
        # Load environment variables
        load_dotenv()
        
        self.api_key = api_key or os.getenv("US_API_KEY")
        self.base_url = base_url or os.getenv("US_API_BASE_URL", "https://api.example.com/economic")
        self.logger = logging.getLogger(__name__)
        
        if not self.api_key:
            self.logger.warning("US API key not provided and not found in environment variables")
        
        # Mapping of metric names to API endpoints
        self.metric_endpoints = {
            "gdp": "gdp",
            "inflation": "cpi",
            "unemployment": "unemployment_rate",
            "interest_rate": "federal_funds_rate",
            # Add more as needed
        }
    
    def fetch(self, metrics: List[str]) -> Dict[str, Any]:
        """Fetch US economic data from API."""
        results = {}
        
        for metric in metrics:
            if metric not in self.metric_endpoints:
                self.logger.warning(f"Metric {metric} not supported for US API")
                continue
                
            endpoint = self.metric_endpoints[metric]
            url = f"{self.base_url}/{endpoint}"
            
            self.logger.info(f"Fetching US data for {metric} from {url}")
            
            try:
                response = requests.get(
                    url,
                    params={
                        "api_key": self.api_key,
                        "frequency": "monthly",
                        "format": "json"
                    },
                    timeout=30
                )
                
                response.raise_for_status()
                data = response.json()
                
                if self.validate_response(data):
                    results[metric] = data
                else:
                    self.logger.warning(f"Invalid response for {metric}")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching {metric}: {str(e)}")
                
        return results
    
    def validate_response(self, response: Any) -> bool:
        """Validate US API response."""
        if not isinstance(response, dict):
            return False
            
        # Check for expected fields in the response
        required_fields = ["data", "frequency", "units"]
        
        for field in required_fields:
            if field not in response:
                return False
                
        # Check that data is not empty
        if not response.get("data"):
            return False
            
        return True