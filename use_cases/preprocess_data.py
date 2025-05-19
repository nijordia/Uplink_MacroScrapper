import logging
from typing import Dict, Any, List

from entities.economic_data import EconomicData
from interfaces.preprocessors.base_preprocessor import BasePreprocessor


class PreprocessDataUseCase:
    """Use case for transforming raw economic data into standardized models."""
    
    def __init__(self, preprocessor: BasePreprocessor):
        self.preprocessor = preprocessor
        self.logger = logging.getLogger(__name__)
    
    def execute(self, country_code: str, raw_data: Dict[str, Any]) -> List[EconomicData]:
        """
        Transform raw data into standardized EconomicData instances.
        
        Args:
            country_code: ISO country code
            raw_data: Dictionary containing raw data from fetcher
            
        Returns:
            List of EconomicData instances
        """
        self.logger.info(f"Preprocessing data for {country_code}")
        
        try:
            processed_data = self.preprocessor.process(country_code, raw_data)
            self.logger.info(f"Successfully preprocessed {len(processed_data)} records for {country_code}")
            return processed_data
        except Exception as e:
            self.logger.error(f"Error preprocessing data for {country_code}: {str(e)}")
            raise