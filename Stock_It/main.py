"""Continuous Stock Price Tracker with Database Integration"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from yahoo_finance_fetcher import YahooFinanceDataFetcher
from marketaux_news_fetcher import MarketauxNewsFetcher
from config import validate_api_keys
from database_models import DatabaseManager, Stock
from etl_pipeline import ETLPipeline
from sentiment_analyzer import SentimentAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('continuous_tracker.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Set UTF-8 encoding for stdout to handle Unicode characters
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

logger = logging.getLogger(__name__)

class ContinuousStockTracker:
    def __init__(self):
        self.etl_pipeline = None
        self.db_manager = None
        self.yahoo_finance = YahooFinanceDataFetcher()
        self.news_api = MarketauxNewsFetcher()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.last_update_time = {}
        self.update_interval = 300
        self.news_update_interval = 1800
        self.last_news_update = datetime.now() - timedelta(hours=1)
        self.companies_to_track = []
        
    def initialize(self):
        try:
            if not validate_api_keys():
                logger.error("API key validation failed")
                return False
            
            logger.info("OK API keys validated successfully")
            
            self.db_manager = DatabaseManager()
            logger.info("OK Database connection established")
            
            self.etl_pipeline = ETLPipeline()
            logger.info("OK ETL Pipeline initialized")

            # Load companies from the database
            self.load_companies_from_db()
            
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            return False

    def load_companies_from_db(self):
        """Load the list of companies to track from the database."""
        session = None
        try:
            logger.info("Loading companies from the database...")
            session = self.db_manager.get_session()
            
            stocks = session.query(Stock.symbol, Stock.company_name).filter(Stock.is_active == True).all()
            
            if stocks:
                self.companies_to_track = [{'symbol': symbol, 'name': name} for symbol, name in stocks]
                logger.info(f"OK Loaded {len(self.companies_to_track)} companies from the database.")
            else:
                logger.warning("No active companies found in the database. Please add some stocks first.")
                
        except Exception as e:
            logger.error(f"Error loading companies from the database: {e}")
            
        finally:
            if session:
                session.close()
    
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
            
            # Use ETL pipeline to fetch and store data
            success = self.etl_pipeline.run_stock_etl(symbol)
            
            if success:
                # Fetch the data again just for display purposes
                stock_data = self.yahoo_finance.get_daily_stock_data(symbol, period='5d')
                if stock_data is not None and not stock_data.empty:
                    logger.info(f"OK Successfully stored {len(stock_data)} days of data for {symbol}")

                    # Display latest data
                    latest_data = stock_data.iloc[-1]  # Most recent data is at the end
                    logger.info(f"  Latest Close: ${latest_data['Close']:.2f}, Volume: {int(latest_data['Volume']):,}")

                    # Update last update time
                    self.last_update_time[symbol] = datetime.now()

                # Also fetch company overview periodically
                company_data = self.yahoo_finance.get_company_overview(symbol)
                if company_data:
                    logger.info(f"  Company: {company_data.get('longName', 'N/A')}, Sector: {company_data.get('sector', 'N/A')}")

                return True
            else:
                logger.warning(f"Failed to store data for {symbol} in database")
                return False
                
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
        
        return False
    
    def fetch_and_store_news_data(self):
        """Fetch news data and store in database with sentiment analysis."""
        try:
            logger.info("Fetching market news...")
            
            # Use ETL pipeline to load news data
            success = self.etl_pipeline.run_news_etl()
            
            if success:
                logger.info("OK Successfully stored news data in database")
                self.last_news_update = datetime.now()
                return True
            else:
                logger.warning("Failed to store news data in database")
                
        except Exception as e:
            logger.error(f"Error fetching news data: {e}")
        
        return False
    
    def run_continuous_tracking(self):
        """Main continuous tracking loop."""
        logger.info("=" * 60)
        logger.info("STARTING CONTINUOUS STOCK TRACKING")
        logger.info("=" * 60)
        
        if not self.initialize():
            logger.error("Failed to initialize. Exiting.")
            return

        if not self.companies_to_track:
            logger.error("No companies to track. Exiting.")
            return
        
        logger.info(f"Tracking {len(self.companies_to_track)} companies:")
        for company in self.companies_to_track:
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
                
                for company in self.companies_to_track:
                    if self.should_update_stock(company['symbol']):
                        if self.fetch_and_store_stock_data(company):
                            stocks_updated += 1
                        
                        # Rate limiting - wait between API calls
                        time.sleep(12)  # Alpha Vantage allows 5 calls per minute
                
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
                self.db_manager.close()
                logger.info("OK Database connections closed")
            
            logger.info(f"Completed {cycle_count} tracking cycles")
            logger.info("Continuous tracking stopped successfully")
            
        except Exception as e:
            logger.error(f"Unexpected error in tracking loop: {e}")
            
            # Cleanup on error
            if self.db_manager:
                self.db_manager.close()

def main():
    """Main function to start continuous tracking."""
    tracker = ContinuousStockTracker()
    tracker.run_continuous_tracking()

if __name__ == "__main__":
    main()