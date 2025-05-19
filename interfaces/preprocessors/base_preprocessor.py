from abc import ABC, abstractmethod
from typing import Dict, Any, List

from entities.economic_data import EconomicData


class BasePreprocessor(ABC):
    """Base interface for all data preprocessors."""
    
    @abstractmethod
    def process(self, country_code: str, raw_data: Dict[str, Any]) -> List[EconomicData]:
        """
        Transform raw data into standardized EconomicData instances.
        
        Args:
            country_code: ISO country code
            raw_data: Dictionary containing raw data from fetcher
            
        Returns:
            List of EconomicData instances
        """
        pass