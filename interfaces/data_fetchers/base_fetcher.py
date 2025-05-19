from abc import ABC, abstractmethod
from typing import Dict, Any, List


class BaseDataFetcher(ABC):
    """Base interface for all data fetchers."""
    
    @abstractmethod
    def fetch(self, metrics: List[str]) -> Dict[str, Any]:
        """
        Fetch raw economic data for specified metrics.
        
        Args:
            metrics: List of metric names to fetch
            
        Returns:
            Dictionary containing raw data
        """
        pass
    
    @abstractmethod
    def validate_response(self, response: Any) -> bool:
        """
        Validate that the response contains expected data.
        
        Args:
            response: Response object from API/scraper
            
        Returns:
            True if valid, False otherwise
        """
        pass