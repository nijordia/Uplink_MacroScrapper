import os
import logging
import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv


class DynamoDBClient:
    """Low-level client for interacting with AWS DynamoDB."""
    
    def __init__(self, table_name: str, region: str = None, profile: Optional[str] = None):
        self.table_name = table_name
        self.logger = logging.getLogger(__name__)
        
        # Load environment variables
        load_dotenv()
        
        # Get AWS credentials and region from environment
        aws_region = region or os.getenv("AWS_REGION", "us-east-1")
        
        # Set up session with credentials from environment
        if profile:
            session = boto3.Session(profile_name=profile)
        else:
            # Boto3 will automatically check environment variables
            # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
            session = boto3.Session()
            
        self.dynamodb = session.resource('dynamodb', region_name=aws_region)
        self.table = self.dynamodb.Table(table_name)
        
    def put_item(self, item: Dict[str, Any]) -> bool:
        """
        Put a single item into DynamoDB.
        
        Args:
            item: Dictionary representation of the item
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.table.put_item(Item=item)
            return True
        except ClientError as e:
            self.logger.error(f"Error putting item: {str(e)}")
            return False
    
    def batch_write_items(self, items: List[Dict[str, Any]]) -> bool:
        """
        Write multiple items to DynamoDB in a batch.
        
        Args:
            items: List of dictionaries representing items
            
        Returns:
            True if all items were written successfully, False otherwise
        """
        if not items:
            return True
            
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            return True
        except ClientError as e:
            self.logger.error(f"Error batch writing items: {str(e)}")
            return False
    
    def query_items(self, key_condition: str, expression_values: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Query items from DynamoDB.
        
        Args:
            key_condition: Key condition expression
            expression_values: Expression attribute values
            
        Returns:
            List of items matching the query
        """
        try:
            response = self.table.query(
                KeyConditionExpression=key_condition,
                ExpressionAttributeValues=expression_values
            )
            return response.get('Items', [])
        except ClientError as e:
            self.logger.error(f"Error querying items: {str(e)}")
            return []
    
    def check_connection(self) -> bool:
        """Check if connection to DynamoDB is working."""
        try:
            # Try to describe the table as a simple connectivity test
            self.dynamodb.meta.client.describe_table(TableName=self.table_name)
            return True
        except ClientError as e:
            self.logger.error(f"Error connecting to DynamoDB: {str(e)}")
            return False