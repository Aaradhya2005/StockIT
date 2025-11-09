import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class for API keys and settings"""
    
    # Alpha Vantage API configuration
    ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
    ALPHA_VANTAGE_BASE_URL = 'https://www.alphavantage.co/query'
    
    # News API configuration
    NEWS_API_KEY = os.getenv('NEWS_API_KEY')
    NEWS_API_BASE_URL = 'https://newsapi.org/v2'
    
    # Default settings
    DEFAULT_STOCK_SYMBOL = 'AAPL'
    DEFAULT_NEWS_SOURCES = 'bloomberg,reuters,financial-times,the-wall-street-journal'
    
    @classmethod
    def validate_keys(cls):
        """Validate that all required API keys are present"""
        missing_keys = []

        # Alpha Vantage is now optional since we use Yahoo Finance for stock data
        # if not cls.ALPHA_VANTAGE_API_KEY:
        #     missing_keys.append('ALPHA_VANTAGE_API_KEY')

        # News API is optional - we can run without news
        # if not cls.NEWS_API_KEY:
        #     missing_keys.append('NEWS_API_KEY')

        if missing_keys:
            raise ValueError(f"Missing API keys: {', '.join(missing_keys)}. Please check your .env file.")

        return True

def validate_api_keys():
    """Standalone function to validate API keys"""
    try:
        return Config.validate_keys()
    except ValueError as e:
        print(f"✗ {e}")
        return False