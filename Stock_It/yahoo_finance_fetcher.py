import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging

class YahooFinanceDataFetcher:
    """Class to fetch stock data from Yahoo Finance API"""
    
    def __init__(self):
        """Initialize the Yahoo Finance data fetcher"""
        self.logger = logging.getLogger(__name__)
    
    def get_daily_stock_data(self, symbol, period='1d'):
        """
        Fetch daily stock data for a given symbol

        Args:
            symbol (str): Stock symbol (e.g., 'AAPL', 'GOOGL')
            period (str): Period to fetch data for ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')

        Returns:
            pd.DataFrame: Stock data as DataFrame with columns [Date, Open, High, Low, Close, Volume] or None if error
        """
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period)

            if data.empty:
                self.logger.warning(f"No data received for {symbol}")
                return None

            # Return DataFrame directly (already has the right format from yfinance)
            # Reset index to make Date a column
            data = data.reset_index()

            return data

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def get_company_overview(self, symbol):
        """
        Fetch company overview data for a given symbol

        Args:
            symbol (str): Stock symbol (e.g., 'AAPL', 'GOOGL')

        Returns:
            dict: Company overview data with keys matching what etl_pipeline expects or None if error
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            if not info:
                self.logger.warning(f"No company info received for {symbol}")
                return None

            # Return format that matches what etl_pipeline.load_company_data expects
            # Keys: longName, sector, exchange, marketCap
            return {
                'symbol': symbol,
                'longName': info.get('longName', symbol),
                'shortName': info.get('shortName', symbol),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'exchange': info.get('exchange', ''),
                'marketCap': info.get('marketCap', 0),
                'currency': info.get('currency', 'USD'),
                'country': info.get('country', ''),
                'description': info.get('longBusinessSummary', ''),
                'sharesOutstanding': info.get('sharesOutstanding', 0),
                'dividendYield': info.get('dividendYield', 0) or 0,
                'trailingEps': info.get('trailingEps', 0) or 0,
                'trailingPE': info.get('trailingPE', 0) or 0,
                'pegRatio': info.get('pegRatio', 0) or 0,
                'bookValue': info.get('bookValue', 0) or 0,
                'dividendRate': info.get('dividendRate', 0) or 0,
                'fiftyTwoWeekHigh': info.get('fiftyTwoWeekHigh', 0) or 0,
                'fiftyTwoWeekLow': info.get('fiftyTwoWeekLow', 0) or 0,
                'fiftyDayAverage': info.get('fiftyDayAverage', 0) or 0,
                'twoHundredDayAverage': info.get('twoHundredDayAverage', 0) or 0,
                'beta': info.get('beta', 0) or 0
            }

        except Exception as e:
            self.logger.error(f"Error fetching company overview for {symbol}: {e}")
            return None
    
    def get_intraday_stock_data(self, symbol, interval='5m', period='1d'):
        """
        Fetch intraday stock data for a given symbol
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL', 'GOOGL')
            interval (str): Data interval ('1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo')
            period (str): Period to fetch data for ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
        
        Returns:
            dict: Intraday stock data or None if error
        """
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval=interval)
            
            if data.empty:
                self.logger.warning(f"No intraday data received for {symbol}")
                return None
            
            # Convert to Alpha Vantage compatible format
            time_series = {}
            for date, row in data.iterrows():
                date_str = date.strftime('%Y-%m-%d %H:%M:%S')
                time_series[date_str] = {
                    '1. open': str(row['Open']),
                    '2. high': str(row['High']),
                    '3. low': str(row['Low']),
                    '4. close': str(row['Close']),
                    '5. volume': str(int(row['Volume']))
                }
            
            return {
                'Meta Data': {
                    '1. Information': f'Intraday ({interval}) open, high, low, close prices and volume',
                    '2. Symbol': symbol,
                    '3. Last Refreshed': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '4. Interval': interval,
                    '5. Output Size': 'Compact',
                    '6. Time Zone': 'US/Eastern'
                },
                f'Time Series ({interval})': time_series
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching intraday data for {symbol}: {e}")
            return None