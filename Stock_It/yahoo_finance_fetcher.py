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
            dict: Stock data in Alpha Vantage compatible format or None if error
        """
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period)
            
            if data.empty:
                self.logger.warning(f"No data received for {symbol}")
                return None
            
            # Convert to Alpha Vantage compatible format
            time_series = {}
            for date, row in data.iterrows():
                date_str = date.strftime('%Y-%m-%d')
                time_series[date_str] = {
                    '1. open': str(row['Open']),
                    '2. high': str(row['High']),
                    '3. low': str(row['Low']),
                    '4. close': str(row['Close']),
                    '5. volume': str(int(row['Volume']))
                }
            
            return {
                'Meta Data': {
                    '1. Information': 'Daily Prices (open, high, low, close) and Volumes',
                    '2. Symbol': symbol,
                    '3. Last Refreshed': datetime.now().strftime('%Y-%m-%d'),
                    '4. Output Size': 'Compact',
                    '5. Time Zone': 'US/Eastern'
                },
                'Time Series (Daily)': time_series
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def get_company_overview(self, symbol):
        """
        Fetch company overview data for a given symbol
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL', 'GOOGL')
        
        Returns:
            dict: Company overview data or None if error
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            if not info:
                self.logger.warning(f"No company info received for {symbol}")
                return None
            
            # Convert to Alpha Vantage compatible format
            return {
                'Symbol': symbol,
                'Name': info.get('longName', 'N/A'),
                'Description': info.get('longBusinessSummary', 'N/A'),
                'Exchange': info.get('exchange', 'N/A'),
                'Currency': info.get('currency', 'USD'),
                'Country': info.get('country', 'N/A'),
                'Sector': info.get('sector', 'N/A'),
                'Industry': info.get('industry', 'N/A'),
                'MarketCapitalization': str(info.get('marketCap', 0)),
                'SharesOutstanding': str(info.get('sharesOutstanding', 0)),
                'DividendYield': str(info.get('dividendYield', 0) or 0),
                'EPS': str(info.get('trailingEps', 0) or 0),
                'PERatio': str(info.get('trailingPE', 0) or 0),
                'PEGRatio': str(info.get('pegRatio', 0) or 0),
                'BookValue': str(info.get('bookValue', 0) or 0),
                'DividendPerShare': str(info.get('dividendRate', 0) or 0),
                'DividendDate': info.get('dividendDate', 'N/A'),
                'ExDividendDate': info.get('exDividendDate', 'N/A'),
                '52WeekHigh': str(info.get('fiftyTwoWeekHigh', 0) or 0),
                '52WeekLow': str(info.get('fiftyTwoWeekLow', 0) or 0),
                '50DayMovingAverage': str(info.get('fiftyDayAverage', 0) or 0),
                '200DayMovingAverage': str(info.get('twoHundredDayAverage', 0) or 0),
                'Beta': str(info.get('beta', 0) or 0)
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