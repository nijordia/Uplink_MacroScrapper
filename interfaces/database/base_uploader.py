from abc import ABC, abstractmethod
from typing import List

from entities.economic_data import EconomicData


class BaseDataUploader(ABC):
    """Base interface for database uploaders."""
    
    @abstractmethod
    def upload(self, data: List[EconomicData]) -> bool:
        """
        Upload economic data to database.
        
        Args:
            data: List of EconomicData instances to upload
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def check_connection(self) -> bool:
        """
        Check if database connection is working.
        
        Returns:
            True if connection works, False otherwise
        """
        pass