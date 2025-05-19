import logging
from typing import List, Dict, Any

from entities.economic_data import EconomicData
from interfaces.database.base_uploader import BaseDataUploader
from interfaces.database.db_model_mapper import DBModelMapper
from frameworks.aws.dynamodb_client import DynamoDBClient


class AWSUploader(BaseDataUploader):
    """Implementation for uploading data to AWS DynamoDB."""
    
    def __init__(self, db_client: DynamoDBClient, model_mapper: DBModelMapper):
        self.db_client = db_client
        self.model_mapper = model_mapper
        self.logger = logging.getLogger(__name__)
    
    def upload(self, data: List[EconomicData]) -> bool:
        """Upload economic data to DynamoDB."""
        if not data:
            return False
            
        self.logger.info(f"Preparing to upload {len(data)} records to DynamoDB")
        
        # Convert domain models to DB items
        db_items = [self.model_mapper.to_db_item(item) for item in data]
        
        # Batch the items if there are many
        batch_size = 25  # DynamoDB batch write limit
        success = True
        
        for i in range(0, len(db_items), batch_size):
            batch = db_items[i:i + batch_size]
            
            try:
                result = self.db_client.batch_write_items(batch)
                if not result:
                    self.logger.error(f"Failed to write batch {i // batch_size + 1}")
                    success = False
            except Exception as e:
                self.logger.error(f"Error uploading batch {i // batch_size + 1}: {str(e)}")
                success = False
        
        return success
    
    def check_connection(self) -> bool:
        """Check if AWS connection is working."""
        try:
            return self.db_client.check_connection()
        except Exception as e:
            self.logger.error(f"Error checking connection: {str(e)}")
            return False