import logging
from typing import List

from entities.economic_data import EconomicData
from interfaces.database.base_uploader import BaseDataUploader


class UploadToDatabaseUseCase:
    """Use case for uploading processed economic data to database."""
    
    def __init__(self, uploader: BaseDataUploader):
        self.uploader = uploader
        self.logger = logging.getLogger(__name__)
    
    def execute(self, data: List[EconomicData]) -> bool:
        """
        Upload processed data to database.
        
        Args:
            data: List of EconomicData instances to upload
            
        Returns:
            True if successful, False otherwise
        """
        if not data:
            self.logger.warning("No data to upload")
            return False
            
        country_code = data[0].country_code if data else "unknown"
        self.logger.info(f"Uploading {len(data)} records for {country_code} to database")
        
        try:
            result = self.uploader.upload(data)
            self.logger.info(f"Successfully uploaded data for {country_code}")
            return result
        except Exception as e:
            self.logger.error(f"Error uploading data: {str(e)}")
            raise