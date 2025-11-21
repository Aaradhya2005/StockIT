"""ETL Pipeline for Stock Tracker System"""

import schedule
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from database_models import (
    db_manager, Stock, StockPrice, FinancialNews, 
    StockNewsRelation, SentimentAnalysis, DailyStockSummary
)
from yahoo_finance_fetcher import YahooFinanceDataFetcher
from marketaux_news_fetcher import MarketauxNewsFetcher
from sentiment_analyzer import SentimentAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('etl_pipeline.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.yahoo_finance = YahooFinanceDataFetcher()
        self.news_api = MarketauxNewsFetcher()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.session = db_manager.get_session()
        self.tracked_symbols = []
        self.load_tracked_symbols()
        logger.info("ETL Pipeline initialized")

    def load_tracked_symbols(self):
        """Load tracked symbols from the database."""
        try:
            stocks = self.session.query(Stock.symbol).filter_by(is_active=True).all()
            self.tracked_symbols = [stock.symbol for stock in stocks]
            logger.info(f"Loaded {len(self.tracked_symbols)} tracked symbols from the database.")
        except Exception as e:
            logger.error(f"Error loading tracked symbols: {e}")
            self.tracked_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX'] # Fallback
    
    def extract_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        try:
            logger.info(f"Extracting stock data for {symbol}")
            # Using yahoo_finance_fetcher now - returns DataFrame directly
            data = self.yahoo_finance.get_daily_stock_data(symbol, period='5d')

            if data is not None and isinstance(data, pd.DataFrame) and not data.empty:
                # Data is already a DataFrame with Date as a column (reset_index was done in fetcher)
                # Rename columns to match database schema
                df = data.copy()
                df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
                df['symbol'] = symbol
                logger.info(f"Extracted {len(df)} records for {symbol}")
                return df

            logger.warning(f"No data received for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error extracting stock data for {symbol}: {e}")
            return None
    
    def extract_company_info(self, symbol: str) -> Optional[Dict]:
        try:
            logger.info(f"Extracting company info for {symbol}")
            overview = self.yahoo_finance.get_company_overview(symbol)
            if overview:
                logger.info(f"Successfully extracted company info for {symbol}")
                return overview
            logger.warning(f"No company info received for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error extracting company info for {symbol}: {e}")
            return None

    def extract_news_data(self, query: str = None, sources: str = None) -> Optional[pd.DataFrame]:
        """Extract news data from News API."""
        try:
            logger.info(f"Extracting news data with query: {query}")
            
            if query:
                data = self.news_api.get_everything_news(query=query, sources=sources, page_size=50)
            else:
                data = self.news_api.get_top_headlines(category='business', page_size=50)
            
            if data is not None:
                df = self.news_api.format_news_to_dataframe(data)
                if df is not None and not df.empty:
                    logger.info(f"Successfully extracted {len(df)} news articles")
                    return df
            
            logger.warning("No news data received for extraction")
            return None
                
        except Exception as e:
            logger.error(f"Error extracting news data: {e}")
            return None
    
    def transform_stock_data(self, data: pd.DataFrame, symbol: str) -> List[Dict]:
        """Transform stock price data for database insertion."""
        try:
            transformed_data = []
            
            for index, row in data.iterrows():
                transformed_record = {
                    'symbol': symbol,
                    'date': row['date'].date() if hasattr(row['date'], 'date') else pd.to_datetime(row['date']).date(),
                    'open_price': float(row['open']),
                    'high_price': float(row['high']),
                    'low_price': float(row['low']),
                    'close_price': float(row['close']),
                    'volume': int(row['volume'])
                }
                
                transformed_record['adjusted_close'] = transformed_record['close_price']
                
                transformed_data.append(transformed_record)
            
            logger.info(f"Transformed {len(transformed_data)} stock records for {symbol}")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error transforming stock data for {symbol}: {e}")
            return []
    
    def transform_news_data(self, data: pd.DataFrame) -> List[Dict]:
        """Transform news data for database insertion."""
        try:
            transformed_data = []
            
            for _, row in data.iterrows():
                published_at = pd.to_datetime(row['publishedAt']).to_pydatetime()
                
                transformed_record = {
                    'title': row['title'][:500] if row['title'] else '',
                    'content': row['content'] if pd.notna(row['content']) else '',
                    'author': row['author'][:255] if pd.notna(row['author']) else None,
                    'published_at': published_at,
                    'url': row['url'][:1000] if row['url'] else None,
                    'source_name': row['source_name'] if 'source_name' in row and pd.notna(row['source_name']) else 'Unknown'
                }
                
                transformed_data.append(transformed_record)
            
            logger.info(f"Transformed {len(transformed_data)} news records")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error transforming news data: {e}")
            return []
    
    def load_stock_data(self, transformed_data: List[Dict]) -> bool:
        """Load stock data into database."""
        try:
            for record in transformed_data:
                stock = self.session.query(Stock).filter_by(symbol=record['symbol']).first()
                if not stock:
                    stock = Stock(
                        symbol=record['symbol'],
                        company_name=record['symbol'],
                        is_active=True
                    )
                    self.session.add(stock)
                    self.session.flush()
                
                existing_price = self.session.query(StockPrice).filter_by(
                    stock_id=stock.stock_id,
                    date=record['date']
                ).first()
                
                if not existing_price:
                    price_record = StockPrice(
                        stock_id=stock.stock_id,
                        date=record['date'],
                        open_price=record['open_price'],
                        high_price=record['high_price'],
                        low_price=record['low_price'],
                        close_price=record['close_price'],
                        adjusted_close=record.get('adjusted_close', record['close_price']),
                        volume=record['volume']
                    )
                    self.session.add(price_record)
            
            self.session.commit()
            logger.info(f"Successfully loaded {len(transformed_data)} stock price records")
            return True
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error loading stock data: {e}")
            return False
    
    def load_company_data(self, company_info: Dict, symbol: str) -> bool:
        """Load/update company information in database."""
        try:
            stock = self.session.query(Stock).filter_by(symbol=symbol).first()
            if stock:
                stock.company_name = company_info.get('longName', symbol)[:255]
                stock.sector = company_info.get('sector', '')[:100]
                stock.exchange = company_info.get('exchange', '')[:50]
                stock.market_cap = int(company_info.get('marketCap', 0))
                
                self.session.commit()
                logger.info(f"Updated company info for {symbol}")
                return True
            else:
                logger.warning(f"Stock {symbol} not found for company info update")
                return False
                
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error loading company data for {symbol}: {e}")
            return False
    
    def load_news_data(self, transformed_data: List[Dict], symbol: str = None) -> bool:
        """Load news data into database.
        
        Args:
            transformed_data: List of news records to load
            symbol: Optional stock symbol to directly link news to
        """
        try:
            news_records_to_process = []
            target_stock = None

            # If symbol is provided, get the stock for direct linking
            if symbol:
                target_stock = self.session.query(Stock).filter_by(symbol=symbol.upper()).first()
                if target_stock:
                    logger.info(f"Will directly link news to stock: {symbol.upper()}")

            for record in transformed_data:
                existing_news = self.session.query(FinancialNews).filter_by(
                    url=record['url']
                ).first()

                if not existing_news and record['url']:
                    news_record = FinancialNews(
                        news_source=record.get('source_name'),
                        title=record['title'],
                        content=record['content'],
                        author=record['author'],
                        published_at=record['published_at'],
                        url=record['url']
                    )

                    # If ETL was called for a specific symbol, set company and symbol now
                    if target_stock:
                        news_record.symbol = target_stock.symbol
                        news_record.company = target_stock.company_name

                    self.session.add(news_record)
                    news_records_to_process.append((news_record, target_stock))

            self.session.flush()  # Flush to get news_id for new records

            for news_record, direct_stock in news_records_to_process:
                self.analyze_and_store_sentiment(news_record)
                # Link news: first try direct link if symbol was provided, then text-based matching
                if direct_stock:
                    self.link_news_to_stock_direct(news_record, direct_stock)
                self.link_news_to_stocks(news_record)
            
            self.session.commit()
            logger.info(f"Successfully loaded and processed {len(news_records_to_process)} news records")
            return True
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error loading news data: {e}")
            return False
    
    def analyze_and_store_sentiment(self, news_record: FinancialNews):
        """Analyze sentiment for a news article and store results."""
        try:
            text_to_analyze = f"{news_record.title} {news_record.content or ''}"
            
            vader_result = self.sentiment_analyzer.analyze_financial_sentiment(
                text_to_analyze, 'vader'
            )
            
            vader_sentiment = SentimentAnalysis(
                news_id=news_record.news_id,
                sentiment_score=vader_result['sentiment_score'],
                sentiment_label=vader_result['sentiment_label'],
                confidence_score=vader_result['confidence_score'],
                analysis_model='VADER'
            )
            self.session.add(vader_sentiment)
            
            textblob_result = self.sentiment_analyzer.analyze_financial_sentiment(
                text_to_analyze, 'textblob'
            )
            
            textblob_sentiment = SentimentAnalysis(
                news_id=news_record.news_id,
                sentiment_score=textblob_result['sentiment_score'],
                sentiment_label=textblob_result['sentiment_label'],
                confidence_score=textblob_result['confidence_score'],
                analysis_model='TextBlob'
            )
            self.session.add(textblob_sentiment)

            # Set aggregated/simple sentiment label on the news record (use VADER result)
            try:
                sentiment_label = vader_result.get('sentiment_label') if vader_result and 'sentiment_label' in vader_result else None
                if sentiment_label:
                    news_record.sentiment = sentiment_label
                    self.session.add(news_record)
                    self.session.flush()
            except Exception:
                # Don't block sentiment storage if this fails
                pass
            
        except Exception as e:
            logger.error(f"Error in sentiment analysis for news ID {news_record.news_id}: {e}")
    
    def link_news_to_stock_direct(self, news_record: FinancialNews, stock: Stock):
        """Directly link a news article to a specific stock."""
        try:
            existing_relation = self.session.query(StockNewsRelation).filter_by(
                stock_id=stock.stock_id,
                news_id=news_record.news_id
            ).first()
            
            if not existing_relation:
                relation = StockNewsRelation(
                    stock_id=stock.stock_id,
                    news_id=news_record.news_id,
                    relevance_score=0.90  # Higher score for direct links
                )
                self.session.add(relation)
                # Update news record with symbol/company if not already set
                if not news_record.symbol:
                    news_record.symbol = stock.symbol
                if not news_record.company:
                    news_record.company = stock.company_name
                self.session.add(news_record)
                self.session.flush()
                logger.debug(f"Directly linked news {news_record.news_id} to stock {stock.symbol}")
        except Exception as e:
            logger.error(f"Error directly linking news to stock for news ID {news_record.news_id}: {e}")
    
    def link_news_to_stocks(self, news_record: FinancialNews):
        """Link news articles to relevant stocks based on content."""
        try:
            text_to_search = f"{news_record.title} {news_record.content or ''}".lower()
            
            stocks = self.session.query(Stock).filter(Stock.is_active==True).all()
            
            for stock in stocks:
                if (stock.symbol.lower() in text_to_search or 
                    stock.company_name.lower() in text_to_search):
                    
                    existing_relation = self.session.query(StockNewsRelation).filter_by(
                        stock_id=stock.stock_id,
                        news_id=news_record.news_id
                    ).first()
                    
                    if not existing_relation:
                        relation = StockNewsRelation(
                            stock_id=stock.stock_id,
                            news_id=news_record.news_id,
                            relevance_score=0.75
                        )
                        self.session.add(relation)
                        # If the news record doesn't yet have symbol/company, set it using this matched stock
                        if not news_record.symbol:
                            news_record.symbol = stock.symbol
                        if not news_record.company:
                            news_record.company = stock.company_name
                        self.session.add(news_record)
                        self.session.flush()
                        logger.debug(f"Linked news {news_record.news_id} to stock {stock.symbol} via text matching")
            
        except Exception as e:
            logger.error(f"Error linking news to stocks for news ID {news_record.news_id}: {e}")
    
    def run_stock_etl(self, symbol: str):
        """Run complete ETL process for a single stock."""
        logger.info(f"Starting ETL process for {symbol}")
        
        try:
            success = False
            
            stock_data = self.extract_stock_data(symbol)
            if stock_data is not None:
                transformed_stock_data = self.transform_stock_data(stock_data, symbol)
                if transformed_stock_data:
                    stock_success = self.load_stock_data(transformed_stock_data)
                    if stock_success:
                        success = True
                        logger.info(f"Successfully loaded stock data for {symbol}")

            company_info = self.extract_company_info(symbol)
            if company_info:
                self.load_company_data(company_info, symbol)
            
            logger.info(f"Completed ETL process for {symbol} - Success: {success}")
            return success
            
        except Exception as e:
            logger.error(f"Error in ETL process for {symbol}: {e}")
            return False
    
    def run_news_etl(self, run_for_all_stocks=False):
        """Run complete ETL process for news data."""
        logger.info("Starting news ETL process")
        
        # Fetch general market news
        news_data = self.extract_news_data()
        if news_data is not None:
            transformed_news_data = self.transform_news_data(news_data)
            if transformed_news_data:
                self.load_news_data(transformed_news_data)  # No symbol for general news
        
        if run_for_all_stocks:
            self.load_tracked_symbols() # Refresh tracked symbols
            for symbol in self.tracked_symbols:
                logger.info(f"Fetching news for symbol: {symbol}")
                stock_news = self.extract_news_data(query=symbol)
                if stock_news is not None:
                    transformed_stock_news = self.transform_news_data(stock_news)
                    if transformed_stock_news:
                        # Pass symbol to directly link news to this stock
                        self.load_news_data(transformed_stock_news, symbol=symbol)
                time.sleep(1) # Rate limiting
        
        logger.info("Completed news ETL process")
        return True
    
    def run_full_etl(self):
        """Run complete ETL process for all tracked stocks and news."""
        logger.info("Starting full ETL pipeline")
        
        try:
            self.load_tracked_symbols() # Refresh tracked symbols
            for symbol in self.tracked_symbols:
                self.run_stock_etl(symbol)
                time.sleep(12)
            
            self.run_news_etl(run_for_all_stocks=True)
            
            logger.info("Full ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Error in full ETL pipeline: {e}")
    
    def schedule_etl_jobs(self):
        """Schedule ETL jobs to run at regular intervals."""
        logger.info("Scheduling ETL jobs")
        
        schedule.every(30).minutes.do(self.run_stock_updates)
        schedule.every(15).minutes.do(lambda: self.run_news_etl(run_for_all_stocks=True))
        schedule.every().day.at("16:30").do(self.run_full_etl)
        
        logger.info("ETL jobs scheduled successfully")
    
    def run_stock_updates(self):
        """Run stock updates only (lighter than full ETL)."""
        logger.info("Running scheduled stock updates")
        self.load_tracked_symbols()
        
        for symbol in self.tracked_symbols:
            try:
                self.run_stock_etl(symbol)
                time.sleep(12)
                
            except Exception as e:
                logger.error(f"Error in stock update for {symbol}: {e}")
    
    def start_scheduler(self):
        """Start the ETL scheduler."""
        logger.info("Starting ETL scheduler")
        
        self.schedule_etl_jobs()
        
        logger.info("Running initial full ETL...")
        self.run_full_etl()
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    def link_existing_news_to_stocks(self):
        """Retroactively link existing news articles to stocks based on content."""
        logger.info("Linking existing news articles to stocks...")
        try:
            # Get all news articles that don't have stock relations yet
            all_news = self.session.query(FinancialNews).all()
            linked_count = 0
            
            for news_record in all_news:
                # Check if this news already has any relations
                existing_relations = self.session.query(StockNewsRelation).filter_by(
                    news_id=news_record.news_id
                ).count()
                
                if existing_relations == 0:
                    # This news hasn't been linked yet, try to link it
                    self.link_news_to_stocks(news_record)
                    linked_count += 1
            
            self.session.commit()
            logger.info(f"Linked {linked_count} existing news articles to stocks")
            return True
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error linking existing news: {e}")
            return False
    
    def close(self):
        """Clean up resources."""
        if self.session:
            self.session.close()
        logger.info("ETL Pipeline closed")

if __name__ == "__main__":
    etl = ETLPipeline()
    
    try:
        db_manager.create_tables()
        etl.start_scheduler()
        
    except KeyboardInterrupt:
        logger.info("ETL Pipeline stopped by user")
    except Exception as e:
        logger.error(f"ETL Pipeline error: {e}")
    finally:
        etl.close()