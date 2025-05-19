import logging
import requests
from typing import Dict, Any, List
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO

from interfaces.data_fetchers.base_fetcher import BaseDataFetcher


class eu_scrapper(BaseDataFetcher):
    """Implementation for scraping EU economic data from websites."""
    
    def __init__(self, base_urls: Dict[str, str]):
        self.base_urls = base_urls  # Dictionary mapping metrics to URLs
        self.logger = logging.getLogger(__name__)
    
    def fetch(self, metrics: List[str]) -> Dict[str, Any]:
        """Fetch EU economic data by scraping websites."""
        results = {}
        
        for metric in metrics:
            if metric not in self.base_urls:
                self.logger.warning(f"Metric {metric} not supported for EU scraper")
                continue
                
            url = self.base_urls[metric]
            self.logger.info(f"Scraping EU data for {metric} from {url}")
            
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # For CSV data
                if url.endswith('.csv'):
                    data = pd.read_csv(StringIO(response.text))
                    results[metric] = {
                        'data': data.to_dict('records'),
                        'format': 'csv'
                    }
                # For HTML tables
                else:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    tables = soup.find_all('table')
                    
                    if tables:
                        data = []
                        for table in tables:
                            table_data = []
                            rows = table.find_all('tr')
                            
                            headers = [header.text.strip() for header in rows[0].find_all(['th', 'td'])]
                            
                            for row in rows[1:]:
                                cells = row.find_all(['td', 'th'])
                                row_data = {
                                    headers[i]: cell.text.strip() 
                                    for i, cell in enumerate(cells) if i < len(headers)
                                }
                                table_data.append(row_data)
                            
                            data.append(table_data)
                        
                        results[metric] = {
                            'data': data,
                            'format': 'html_table'
                        }
                    else:
                        self.logger.warning(f"No tables found for {metric}")
                
                if metric in results and not self.validate_response(results[metric]):
                    del results[metric]
                    self.logger.warning(f"Invalid response for {metric}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error scraping {metric}: {str(e)}")
                
        return results
    
    def validate_response(self, response: Any) -> bool:
        """Validate scraped response."""
        if not isinstance(response, dict):
            return False
            
        # Check for expected fields in the response
        if 'data' not in response or 'format' not in response:
            return False
            
        # Check that data is not empty
        if not response.get('data'):
            return False
            
        return True