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
    db_manager, Stock, StockPrice, NewsSource, FinancialNews, 
    StockNewsRelation, SentimentAnalysis, DailyStockSummary
)
from yahoo_finance_fetcher import YahooFinanceDataFetcher
from news_api_fetcher import NewsAPIFetcher
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
        self.news_api = NewsAPIFetcher()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.session = db_manager.get_session()
        self.tracked_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
        logger.info("ETL Pipeline initialized")
    
    def extract_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        try:
            logger.info(f"Extracting stock data for {symbol}")
            data = self.yahoo_finance.get_daily_stock_data(symbol, period='5d')
            
            if data and 'Time Series (Daily)' in data:
                df_data = []
                for date_str, values in data['Time Series (Daily)'].items():
                    df_data.append({
                        'date': pd.to_datetime(date_str),
                        'open': float(values['1. open']),
                        'high': float(values['2. high']),
                        'low': float(values['3. low']),
                        'close': float(values['4. close']),
                        'volume': int(values['5. volume']),
                        'symbol': symbol
                    })
                
                df = pd.DataFrame(df_data).sort_values('date')
                if not df.empty:
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
                # Convert dict to DataFrame
                df = self.news_api.format_news_to_dataframe(data)
                if df is not None and not df.empty:
                    logger.info(f"Successfully extracted {len(df)} news articles")
                    return df
                else:
                    logger.warning("No news data received")
                    return None
            else:
                logger.warning("No news data received")
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
                    'date': row['date'].date() if hasattr(row['date'], 'date') else row['date'],
                    'open_price': float(row['open']),
                    'high_price': float(row['high']),
                    'low_price': float(row['low']),
                    'close_price': float(row['close']),
                    'volume': int(row['volume'])
                }
                
                # Set adjusted close to close price for Yahoo Finance data
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
                # Parse published date
                published_at = pd.to_datetime(row['publishedAt']).to_pydatetime()
                
                transformed_record = {
                    'title': row['title'][:500] if row['title'] else '',
                    'content': row['content'] if pd.notna(row['content']) else '',
                    'author': row['author'][:255] if pd.notna(row['author']) else None,
                    'published_at': published_at,
                    'url': row['url'][:1000] if row['url'] else None,
                    'source_name': row['source_name'] if 'source_name' in row else 'Unknown'
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
                # Get or create stock
                stock = self.session.query(Stock).filter_by(symbol=record['symbol']).first()
                if not stock:
                    # Create new stock record (basic info, will be updated later)
                    stock = Stock(
                        symbol=record['symbol'],
                        company_name=record['symbol'],  # Placeholder
                        is_active=True
                    )
                    self.session.add(stock)
                    self.session.flush()  # Get the stock_id
                
                # Check if price record already exists
                existing_price = self.session.query(StockPrice).filter_by(
                    stock_id=stock.stock_id,
                    date=record['date']
                ).first()
                
                if not existing_price:
                    # Create new price record
                    price_record = StockPrice(
                        stock_id=stock.stock_id,
                        date=record['date'],
                        open_price=record['open_price'],
                        high_price=record['high_price'],
                        low_price=record['low_price'],
                        close_price=record['close_price'],
                        adjusted_close=record['adjusted_close'],
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
                # Update existing stock with company info
                stock.company_name = company_info.get('Name', symbol)[:255]
                stock.sector = company_info.get('Sector', '')[:100]
                stock.exchange = company_info.get('Exchange', '')[:50]
                
                # Parse market cap
                market_cap_str = company_info.get('MarketCapitalization', '0')
                try:
                    stock.market_cap = int(market_cap_str) if market_cap_str != 'None' else None
                except (ValueError, TypeError):
                    stock.market_cap = None
                
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
    
    def load_news_data(self, transformed_data: List[Dict]) -> bool:
        """Load news data into database."""
        try:
            for record in transformed_data:
                # Get or create news source
                source = self.session.query(NewsSource).filter_by(
                    source_name=record['source_name']
                ).first()
                
                if not source:
                    source = NewsSource(
                        source_name=record['source_name'],
                        credibility_score=0.50,  # Default credibility
                        is_active=True
                    )
                    self.session.add(source)
                    self.session.flush()
                
                # Check if news article already exists
                existing_news = self.session.query(FinancialNews).filter_by(
                    url=record['url']
                ).first()
                
                if not existing_news and record['url']:
                    # Create new news record
                    news_record = FinancialNews(
                        source_id=source.source_id,
                        title=record['title'],
                        content=record['content'],
                        author=record['author'],
                        published_at=record['published_at'],
                        url=record['url']
                    )
                    self.session.add(news_record)
                    self.session.flush()
                    
                    # Perform sentiment analysis
                    self.analyze_and_store_sentiment(news_record)
            
            self.session.commit()
            logger.info(f"Successfully loaded {len(transformed_data)} news records")
            return True
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error loading news data: {e}")
            return False
    
    def analyze_and_store_sentiment(self, news_record: FinancialNews):
        """Analyze sentiment for a news article and store results."""
        try:
            # Combine title and content for analysis
            text_to_analyze = f"{news_record.title} {news_record.content or ''}"
            
            # Analyze with VADER
            vader_result = self.sentiment_analyzer.analyze_financial_sentiment(
                text_to_analyze, 'vader'
            )
            
            # Store VADER sentiment
            vader_sentiment = SentimentAnalysis(
                news_id=news_record.news_id,
                sentiment_score=vader_result['sentiment_score'],
                sentiment_label=vader_result['sentiment_label'],
                confidence_score=vader_result['confidence_score'],
                analysis_model='VADER'
            )
            self.session.add(vader_sentiment)
            
            # Analyze with TextBlob
            textblob_result = self.sentiment_analyzer.analyze_financial_sentiment(
                text_to_analyze, 'textblob'
            )
            
            # Store TextBlob sentiment
            textblob_sentiment = SentimentAnalysis(
                news_id=news_record.news_id,
                sentiment_score=textblob_result['sentiment_score'],
                sentiment_label=textblob_result['sentiment_label'],
                confidence_score=textblob_result['confidence_score'],
                analysis_model='TextBlob'
            )
            self.session.add(textblob_sentiment)
            
            logger.info(f"Sentiment analysis completed for news ID: {news_record.news_id}")
            
        except Exception as e:
            logger.error(f"Error in sentiment analysis for news ID {news_record.news_id}: {e}")
    
    def link_news_to_stocks(self, news_record: FinancialNews):
        """Link news articles to relevant stocks based on content."""
        try:
            text_to_search = f"{news_record.title} {news_record.content or ''}".lower()
            
            # Get all active stocks
            stocks = self.session.query(Stock).filter_by(is_active=True).all()
            
            for stock in stocks:
                # Check if stock symbol or company name is mentioned
                if (stock.symbol.lower() in text_to_search or 
                    stock.company_name.lower() in text_to_search):
                    
                    # Check if relation already exists
                    existing_relation = self.session.query(StockNewsRelation).filter_by(
                        stock_id=stock.stock_id,
                        news_id=news_record.news_id
                    ).first()
                    
                    if not existing_relation:
                        # Create stock-news relation
                        relation = StockNewsRelation(
                            stock_id=stock.stock_id,
                            news_id=news_record.news_id,
                            relevance_score=0.75  # Default relevance
                        )
                        self.session.add(relation)
            
            logger.info(f"Linked news ID {news_record.news_id} to relevant stocks")
            
        except Exception as e:
            logger.error(f"Error linking news to stocks for news ID {news_record.news_id}: {e}")
    
    def run_stock_etl(self, symbol: str):
        """Run complete ETL process for a single stock."""
        logger.info(f"Starting ETL process for {symbol}")
        
        try:
            success = False
            
            # Extract and load stock price data
            stock_data = self.extract_stock_data(symbol)
            if stock_data is not None:
                logger.info(f"Extracted stock data for {symbol}: {len(stock_data)} rows")
                transformed_stock_data = self.transform_stock_data(stock_data, symbol)
                if transformed_stock_data:
                    logger.info(f"Transformed stock data for {symbol}: {len(transformed_stock_data)} records")
                    stock_success = self.load_stock_data(transformed_stock_data)
                    if stock_success:
                        success = True
                        logger.info(f"Successfully loaded stock data for {symbol}")
                    else:
                        logger.warning(f"Failed to load stock data for {symbol}")
                else:
                    logger.warning(f"No transformed stock data for {symbol}")
            else:
                logger.warning(f"No stock data extracted for {symbol}")
            
            # Extract and load company information
            company_info = self.extract_company_info(symbol)
            if company_info:
                logger.info(f"Extracted company info for {symbol}")
                company_success = self.load_company_data(company_info, symbol)
                if company_success:
                    logger.info(f"Successfully loaded company data for {symbol}")
                else:
                    logger.warning(f"Failed to load company data for {symbol}")
            else:
                logger.warning(f"No company info extracted for {symbol}")
            
            logger.info(f"Completed ETL process for {symbol} - Success: {success}")
            return success
            
        except Exception as e:
            logger.error(f"Error in ETL process for {symbol}: {e}")
            return False
    
    def run_news_etl(self):
        """Run complete ETL process for news data."""
        logger.info("Starting news ETL process")
        
        # Extract general business news
        news_data = self.extract_news_data()
        if news_data is not None:
            transformed_news_data = self.transform_news_data(news_data)
            if transformed_news_data:
                self.load_news_data(transformed_news_data)
        
        # Extract stock-specific news for tracked symbols
        for symbol in self.tracked_symbols:
            stock_news = self.extract_news_data(query=symbol)
            if stock_news is not None:
                transformed_stock_news = self.transform_news_data(stock_news)
                if transformed_stock_news:
                    self.load_news_data(transformed_stock_news)
        
        logger.info("Completed news ETL process")
    
    def run_full_etl(self):
        """Run complete ETL process for all tracked stocks and news."""
        logger.info("Starting full ETL pipeline")
        
        try:
            # Process each tracked stock
            for symbol in self.tracked_symbols:
                self.run_stock_etl(symbol)
                time.sleep(1)  # Rate limiting
            
            # Process news data
            self.run_news_etl()
            
            logger.info("Full ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Error in full ETL pipeline: {e}")
    
    def schedule_etl_jobs(self):
        """Schedule ETL jobs to run at regular intervals."""
        logger.info("Scheduling ETL jobs")
        
        # Schedule stock data updates (every 30 minutes during market hours)
        schedule.every(30).minutes.do(self.run_stock_updates)
        
        # Schedule news updates (every 15 minutes)
        schedule.every(15).minutes.do(self.run_news_etl)
        
        # Schedule full ETL (once daily at market close)
        schedule.every().day.at("16:30").do(self.run_full_etl)
        
        logger.info("ETL jobs scheduled successfully")
    
    def run_stock_updates(self):
        """Run stock updates only (lighter than full ETL)."""
        logger.info("Running scheduled stock updates")
        
        for symbol in self.tracked_symbols:
            try:
                stock_data = self.extract_stock_data(symbol)
                if stock_data is not None:
                    # Only get the latest data (last 5 days)
                    recent_data = stock_data.head(5)
                    transformed_data = self.transform_stock_data(recent_data, symbol)
                    if transformed_data:
                        self.load_stock_data(transformed_data)
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error in stock update for {symbol}: {e}")
    
    def start_scheduler(self):
        """Start the ETL scheduler."""
        logger.info("Starting ETL scheduler")
        
        # Schedule jobs
        self.schedule_etl_jobs()
        
        # Run initial ETL
        self.run_full_etl()
        
        # Keep scheduler running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def close(self):
        """Clean up resources."""
        if self.session:
            self.session.close()
        logger.info("ETL Pipeline closed")

if __name__ == "__main__":
    # Initialize and run ETL pipeline
    etl = ETLPipeline()
    
    try:
        # Create database tables if they don't exist
        db_manager.create_tables()
        
        # Start the scheduler
        etl.start_scheduler()
        
    except KeyboardInterrupt:
        logger.info("ETL Pipeline stopped by user")
    except Exception as e:
        logger.error(f"ETL Pipeline error: {e}")
    finally:
        etl.close()