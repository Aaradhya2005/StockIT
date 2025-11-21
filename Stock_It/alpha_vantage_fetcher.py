import requests
import pandas as pd
import json
from datetime import datetime
from config import Config

class AlphaVantageDataFetcher:
    """Class to fetch stock data from Alpha Vantage API"""
    
    def __init__(self):
        """Initialize the Alpha Vantage data fetcher"""
        Config.validate_keys()
        self.api_key = Config.ALPHA_VANTAGE_API_KEY
        self.base_url = Config.ALPHA_VANTAGE_BASE_URL
    
    def get_daily_stock_data(self, symbol, outputsize='compact'):
        """
        Fetch daily stock data for a given symbol
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL', 'GOOGL')
            outputsize (str): 'compact' for last 100 data points, 'full' for full data
        
        Returns:
            dict: Stock data or None if error
        """
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': outputsize,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'Error Message' in data:
                print(f"Error: {data['Error Message']}")
                return None
            
            if 'Note' in data:
                print(f"API Limit Notice: {data['Note']}")
                return None
            
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return None
    
    def get_intraday_stock_data(self, symbol, interval='5min'):
        """
        Fetch intraday stock data for a given symbol
        
        Args:
            symbol (str): Stock symbol
            interval (str): Time interval ('1min', '5min', '15min', '30min', '60min')
        
        Returns:
            dict: Intraday stock data or None if error
        """
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol,
            'interval': interval,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'Error Message' in data:
                print(f"Error: {data['Error Message']}")
                return None
            
            if 'Note' in data:
                print(f"API Limit Notice: {data['Note']}")
                return None
            
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return None
    
    def get_company_overview(self, symbol):
        """
        Fetch company overview data
        
        Args:
            symbol (str): Stock symbol
        
        Returns:
            dict: Company overview data or None if error
        """
        params = {
            'function': 'OVERVIEW',
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'Error Message' in data:
                print(f"Error: {data['Error Message']}")
                return None
            
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return None
    
    def format_daily_data_to_dataframe(self, data):
        """
        Convert daily stock data to pandas DataFrame
        
        Args:
            data (dict): Raw API response data
        
        Returns:
            pandas.DataFrame: Formatted stock data
        """
        if not data or 'Time Series (Daily)' not in data:
            return None
        
        time_series = data['Time Series (Daily)']
        df = pd.DataFrame.from_dict(time_series, orient='index')
        
        # Rename columns
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        # Convert to numeric
        df = df.astype(float)
        
        # Convert index to datetime
        df.index = pd.to_datetime(df.index)
        
        # Sort by date
        df = df.sort_index()
        
        return df
    
    def save_data_to_csv(self, data, filename):
        """
        Save stock data to CSV file
        
        Args:
            data (pandas.DataFrame): Stock data
            filename (str): Output filename
        """
        if data is not None:
            data.to_csv(filename)
            print(f"Data saved to {filename}")
        else:
            print("No data to save")

# Example usage
if __name__ == "__main__":
    # Initialize the fetcher
    fetcher = AlphaVantageDataFetcher()
    
    # Example: Fetch daily data for Apple
    symbol = "AAPL"
    print(f"Fetching daily data for {symbol}...")
    
    raw_data = fetcher.get_daily_stock_data(symbol)
    if raw_data:
        df = fetcher.format_daily_data_to_dataframe(raw_data)
        if df is not None:
            print(f"Successfully fetched {len(df)} days of data")
            print(df.head())
            
            # Save to CSV
            fetcher.save_data_to_csv(df, f"{symbol}_daily_data.csv")
        else:
            print("Failed to format data")
    else:
        print("Failed to fetch data")