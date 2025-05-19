import os
import logging
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from dotenv import load_dotenv
import json

from interfaces.data_fetchers.base_fetcher import BaseDataFetcher


class cl_api_fetcher(BaseDataFetcher):
    """Implementation for fetching Chilean economic data from Central Bank API."""
    
    BASE_URL = "https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx"
    
    def __init__(self, series_mappings: Dict[str, str] = None):
        """
        Initialize Chile API Fetcher.
        
        Args:
            series_mappings: Dictionary mapping metric names to Central Bank series IDs
        """
        self.logger = logging.getLogger(__name__)
        
        # Load environment variables
        load_dotenv()
        self.user = os.getenv("CHILE_API_EMAIL")
        self.password = os.getenv("CHILE_API_PASSWORD")
        
        if not self.user or not self.password:
            self.logger.warning("Chile API credentials not found in .env file")
            
        # Load indicator metadata from config file
        with open("config/cl_indicator_metadata.json", "r", encoding="utf-8") as f:
            self.indicator_metadata = json.load(f)

        # Build series_mappings from metadata
        self.series_mappings = {
            metric: meta["id"]
            for metric, meta in self.indicator_metadata.items()
            if "id" in meta
        }
    
    def fetch(self, metrics: List[str]) -> Dict[str, Any]:
        """
        Fetch Chilean economic data for specified metrics.
        
        Args:
            metrics: List of metric names to fetch
            
        Returns:
            Dictionary containing raw data
        """
        if metrics is None:
            metrics = list(self.series_mappings.keys())
            
        results = {}
        
        if not self.user or not self.password:
            self.logger.error("Cannot fetch data: missing API credentials")
            return results
            
        series_list = []
        for metric in metrics:
            if metric in self.series_mappings:
                series_list.append(self.series_mappings[metric])
            else:
                self.logger.warning(f"Metric {metric} not supported for Chile API")
        
        if not series_list:
            return results
        
        self.logger.info(f"Fetching {len(series_list)} series from Chile Central Bank API")
        
        # Here is where the amount of fetched data is defined: Get last 5 years of data
        try:
            # Calculate date ranges
            today = datetime.today().strftime('%Y-%m-%d')
            five_years_ago = f"{datetime.today().year - 5}-{datetime.today().month:02d}-{datetime.today().day:02d}"
            
            df_data = self.get_multiple_series(
                series_list,
                desde=five_years_ago,
                hasta=today
            )
            
            if df_data is not None:
                with open("config/cl_indicator_metadata.json", "r", encoding="utf-8") as metadata_f:
                    metadata = json.load(metadata_f)
                # Process the DataFrame into a format suitable for the preprocessor
                for metric in metrics:
                    if metric in self.series_mappings:
                        series_id = self.series_mappings[metric]
                        if series_id in df_data.columns:
                            # Extract data for this metric
                            metric_data = df_data[series_id].dropna()

                            frequency = metadata.get(metric, {}).get("frequency", "monthly")
                            
                            # Format data
                            results[metric] = {
                                "data": [
                                    {
                                        "date": index.strftime('%Y-%m-%d'),
                                        "value": str(value)
                                    }
                                    for index, value in metric_data.items()
                                ],
                                "frequency":  frequency,
                                "units": self._get_unit_for_metric(metric),
                                "source": "Banco Central de Chile"
                            }
                            
                            self.logger.info(f"Successfully fetched {len(results[metric]['data'])} records for {metric}")
        
        except Exception as e:
            self.logger.error(f"Error fetching data from Chile API: {str(e)}")
        
        return results
    
    def validate_response(self, response: Any) -> bool:
        """Validate Chile API response."""
        if not isinstance(response, dict):
            return False
        
        # Central Bank API returns Codigo=0 for success
        if response.get('Codigo') != 0:
            return False
            
        # Check for data
        if 'Series' not in response or 'Obs' not in response['Series']:
            return False
            
        # Check that observations are not empty
        if not response['Series']['Obs']:
            return False
            
        return True
    
    def get_series(self, timeseries, firstdate=None, lastdate=None):
        """Get data for a specific time series from Chile Central Bank."""
        params = {
            'user': self.user,
            'pass': self.password,
            'function': 'GetSeries',
            'timeseries': timeseries
        }
        
        if firstdate:
            params['firstdate'] = firstdate
            
        if lastdate:
            params['lastdate'] = lastdate
        
        self.logger.debug(f"Requesting series {timeseries}")
        
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not self.validate_response(data):
                self.logger.warning(f"Invalid response for series {timeseries}")
                return None
                
            return data
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching series {timeseries}: {str(e)}")
            return None
    
    def get_multiple_series(self, series_list, desde=None, hasta=None):
        """Get multiple series and combine them into a DataFrame."""
        results = {}
        
        for series_id in series_list:
            data = self.get_series(series_id, firstdate=desde, lastdate=hasta)
            
            if data and data.get('Codigo') == 0:
                series_name = data['Series']['descripEsp']
                observations = data['Series']['Obs']
                
                # Extract dates and values
                dates = []
                values = []
                for obs in observations:
                    date_str = obs['indexDateString']
                    try:
                        date = datetime.strptime(date_str, '%d-%m-%Y')
                        value = float(obs['value']) if obs['value'] else None
                        
                        dates.append(date)
                        values.append(value)
                    except (ValueError, TypeError) as e:
                        self.logger.warning(f"Error parsing data point {date_str}: {str(e)}")
                
                # Create Series
                results[series_id] = pd.Series(values, index=dates, name=series_name)
        
        # Combine all series into a DataFrame
        if results:
            return pd.DataFrame(results)
        return None
    
    def _get_unit_for_metric(self, metric: str) -> str:
        return self.indicator_metadata.get(metric, {}).get("unit", "")