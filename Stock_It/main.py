"""Continuous Stock Price Tracker with Database Integration"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from alpha_vantage_fetcher import AlphaVantageDataFetcher
from news_api_fetcher import NewsAPIFetcher
from config import validate_api_keys
from database_models import DatabaseManager
from etl_pipeline import ETLPipeline
from sentiment_analyzer import SentimentAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('continuous_tracker.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

TOP_20_COMPANIES = [
    {'symbol': 'AAPL', 'name': 'Apple Inc'},
    {'symbol': 'MSFT', 'name': 'Microsoft Corporation'},
    {'symbol': 'GOOGL', 'name': 'Alphabet Inc'},
    {'symbol': 'AMZN', 'name': 'Amazon.com Inc'},
    {'symbol': 'NVDA', 'name': 'NVIDIA Corporation'},
    {'symbol': 'TSLA', 'name': 'Tesla Inc'},
    {'symbol': 'META', 'name': 'Meta Platforms Inc'},
    {'symbol': 'BRK.B', 'name': 'Berkshire Hathaway Inc'},
    {'symbol': 'UNH', 'name': 'UnitedHealth Group Inc'},
    {'symbol': 'JNJ', 'name': 'Johnson & Johnson'},
    {'symbol': 'JPM', 'name': 'JPMorgan Chase & Co'},
    {'symbol': 'V', 'name': 'Visa Inc'},
    {'symbol': 'PG', 'name': 'Procter & Gamble Co'},
    {'symbol': 'XOM', 'name': 'Exxon Mobil Corporation'},
    {'symbol': 'HD', 'name': 'Home Depot Inc'},
    {'symbol': 'CVX', 'name': 'Chevron Corporation'},
    {'symbol': 'MA', 'name': 'Mastercard Inc'},
    {'symbol': 'BAC', 'name': 'Bank of America Corp'},
    {'symbol': 'ABBV', 'name': 'AbbVie Inc'},
    {'symbol': 'PFE', 'name': 'Pfizer Inc'},
    {'symbol': 'KO', 'name': 'The Coca-Cola Company'},
    {'symbol': 'AVGO', 'name': 'Broadcom Inc'},
    {'symbol': 'TMO', 'name': 'Thermo Fisher Scientific Inc'},
    {'symbol': 'COST', 'name': 'Costco Wholesale Corporation'},
    {'symbol': 'WMT', 'name': 'Walmart Inc'},
    {'symbol': 'LLY', 'name': 'Eli Lilly and Company'},
    {'symbol': 'NFLX', 'name': 'Netflix Inc'},
    {'symbol': 'ADBE', 'name': 'Adobe Inc'},
    {'symbol': 'CRM', 'name': 'Salesforce Inc'},
    {'symbol': 'ORCL', 'name': 'Oracle Corporation'},
    {'symbol': 'ACN', 'name': 'Accenture plc'}
]

class ContinuousStockTracker:
    def __init__(self):
        self.etl_pipeline = None
        self.db_manager = None
        self.alpha_vantage = AlphaVantageDataFetcher()
        self.news_api = NewsAPIFetcher()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.last_update_time = {}
        self.update_interval = 300
        self.news_update_interval = 1800
        self.last_news_update = datetime.now() - timedelta(hours=1)
        
    def initialize(self):
        try:
            if not validate_api_keys():
                logger.error("API key validation failed")
                return False
            
            logger.info("API keys validated successfully")
            
            self.db_manager = DatabaseManager()
            logger.info("Database connection established")
            
            self.etl_pipeline = ETLPipeline()
            logger.info("ETL Pipeline initialized")
            
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            return False
    
    def should_update_stock(self, symbol):
        """Check if stock data should be updated based on time interval."""
        if symbol not in self.last_update_time:
            return True
        
        time_since_update = datetime.now() - self.last_update_time[symbol]
        return time_since_update.total_seconds() >= self.update_interval
    
    def should_update_news(self):
        """Check if news data should be updated."""
        time_since_update = datetime.now() - self.last_news_update
        return time_since_update.total_seconds() >= self.news_update_interval
    
    def fetch_and_store_stock_data(self, company):
        """Fetch stock data for a company and store in database."""
        symbol = company['symbol']
        name = company['name']
        
        try:
            logger.info(f"Fetching data for {symbol} ({name})")
            
            # Retry ETL/network calls on transient failures
            max_attempts = 3
            success = False
            for attempt in range(1, max_attempts + 1):
                try:
                    logger.info(f"Starting ETL process for {symbol} (attempt {attempt}/{max_attempts})")
                    success = self.etl_pipeline.run_stock_etl(symbol)
                    if success:
                        break
                    else:
                        logger.warning(f"ETL reported failure for {symbol} on attempt {attempt}")
                except Exception as e:
                    logger.error(f"ETL/network error for {symbol} on attempt {attempt}: {e}")
                
                if attempt < max_attempts:
                    backoff = 2 ** attempt
                    logger.info(f"Retrying {symbol} after {backoff}s backoff...")
                    time.sleep(backoff)
            
            if success:
                # Fetch the data again just for display purposes
                stock_data = self.alpha_vantage.get_daily_stock_data(symbol, outputsize='compact')
                if stock_data is not None:
                    df = self.alpha_vantage.format_daily_data_to_dataframe(stock_data)
                    if df is not None and not df.empty:
                        logger.info(f"Successfully stored {len(df)} days of data for {symbol}")
                        
                        # Display latest data
                        latest_data = df.iloc[-1]  # Most recent data is at the end after sorting
                        logger.info(f"  Latest Close: ${latest_data['Close']}, Volume: {latest_data['Volume']:,}")
                        
                        # Update last update time
                        self.last_update_time[symbol] = datetime.now()
                
                # Also fetch company overview periodically
                try:
                    company_data = self.alpha_vantage.get_company_overview(symbol)
                    if company_data:
                        logger.info(f"  Company: {company_data.get('Name', 'N/A')}, Sector: {company_data.get('Sector', 'N/A')}")
                except Exception as e:
                    logger.error(f"Error fetching company overview for {symbol}: {e}")
                
                return True
            else:
                logger.warning(f"Failed to store data for {symbol} in database after {max_attempts} attempts")
                return False
                
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
        
        return False
    
    def fetch_and_store_news_data(self):
        """Fetch news data and store in database with sentiment analysis."""
        try:
            logger.info("Fetching market news...")
            
            # Fetch general market news
            market_news = self.news_api.get_financial_market_news()
            
            if market_news:
                logger.info(f"Fetched {len(market_news)} market news articles")
                
                # Use ETL pipeline to load news data
                success = self.etl_pipeline.run_news_etl()
                
                if success:
                    logger.info("Successfully stored news data in database")
                    self.last_news_update = datetime.now()
                    return True
                else:
                    logger.warning("Failed to store news data in database")
            else:
                logger.warning("No news data received")
                
        except Exception as e:
            logger.error(f"Error fetching news data: {e}")
        
        return False
    
    def run_continuous_tracking(self):
        """Main continuous tracking loop."""
        logger.info("=" * 60)
        logger.info("STARTING CONTINUOUS STOCK TRACKING FOR TOP 20 COMPANIES")
        logger.info("=" * 60)
        
        if not self.initialize():
            logger.error("Failed to initialize. Exiting.")
            return
        
        logger.info(f"Tracking {len(TOP_20_COMPANIES)} companies:")
        for company in TOP_20_COMPANIES:
            logger.info(f"  - {company['symbol']}: {company['name']}")
        
        logger.info(f"Update intervals: Stock data every {self.update_interval}s, News every {self.news_update_interval}s")
        logger.info("Press Ctrl+C to stop tracking...\n")
        
        cycle_count = 0
        
        try:
            while True:
                cycle_count += 1
                cycle_start_time = datetime.now()
                
                logger.info(f"=== Cycle {cycle_count} started at {cycle_start_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
                
                # Track stocks that need updating
                stocks_updated = 0
                
                for company in TOP_20_COMPANIES:
                    if self.should_update_stock(company['symbol']):
                        if self.fetch_and_store_stock_data(company):
                            stocks_updated += 1
                        
                        # Rate limiting - wait between API calls (increased from 12s to reduce resets)
                        time.sleep(15)  # Alpha Vantage allows 5 calls per minute
                
                # Update news if needed
                if self.should_update_news():
                    logger.info("Updating news data...")
                    self.fetch_and_store_news_data()
                
                cycle_end_time = datetime.now()
                cycle_duration = (cycle_end_time - cycle_start_time).total_seconds()
                
                logger.info(f"=== Cycle {cycle_count} completed in {cycle_duration:.1f}s ===")
                logger.info(f"Updated {stocks_updated} stocks this cycle")
                logger.info(f"Next cycle in {self.update_interval}s...\n")
                
                # Wait before next cycle
                time.sleep(self.update_interval)
                
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 60)
            logger.info("STOPPING CONTINUOUS TRACKING")
            logger.info("=" * 60)
            
            # Cleanup
            if self.db_manager:
                self.db_manager.close_all_sessions()
                logger.info("Database connections closed")
            
            logger.info(f"Completed {cycle_count} tracking cycles")
            logger.info("Continuous tracking stopped successfully")
            
        except Exception as e:
            logger.error(f"Unexpected error in tracking loop: {e}")
            
            # Cleanup on error
            if self.db_manager:
                self.db_manager.close_all_sessions()

def main():
    """Main function to start continuous tracking."""
    tracker = ContinuousStockTracker()
    tracker.run_continuous_tracking()

if __name__ == "__main__":
    main()