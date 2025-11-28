#!/usr/bin/env python3
"""
News Viewer Script - Displays financial news stored in the database
with formatting similar to the news fetchers' output format.
"""

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from database_models import db_manager
import pandas as pd

class NewsViewer:
    def __init__(self):
        """Initialize the news viewer with database connection."""
        self.db_manager = db_manager
    
    def connect_to_database(self):
        """Connect to the database using psycopg2 for direct queries."""
        load_dotenv()
        
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'stock_tracker_db'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD')
            )
            return conn
        except Exception as e:
            print(f"Database connection error: {e}")
            print("\nMake sure PostgreSQL is running and your .env file has correct credentials.")
            return None
    
    def view_all_news(self, limit=20, days_back=7):
        """View all recent news articles with sentiment analysis."""
        conn = self.connect_to_database()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        print("=" * 80)
        print("FINANCIAL NEWS OVERVIEW")
        print("=" * 80)
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Showing news from last {days_back} days (Limit: {limit})")
        
        try:
            # Get total news count
            cursor.execute('SELECT COUNT(*) FROM financial_news;')
            total_news = cursor.fetchone()[0]
            print(f"Total news articles in database: {total_news:,}")
            
            # Try with date filtering first, fallback to all news if no results
            try:
                cursor.execute("""
                    SELECT 
                        fn.title,
                        fn.content,
                        fn.published_at,
                        fn.url,
                        fn.news_source,
                        COALESCE(sa.sentiment_label, 'Not Analyzed') as sentiment_label,
                        COALESCE(sa.sentiment_score, 0.0) as sentiment_score,
                        COALESCE(sa.confidence_score, 0.0) as confidence_score,
                        COALESCE(sa.analysis_model, 'N/A') as analysis_model
                    FROM financial_news fn
                    LEFT JOIN sentiment_analysis sa ON fn.news_id = sa.news_id
                    WHERE fn.published_at >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER BY fn.published_at DESC
                    LIMIT %s;
                """, (days_back, limit))
            except Exception as e:
                print(f"Date filtering failed: {e}, showing all news...")
                cursor.execute("""
                    SELECT 
                        fn.title,
                        fn.content,
                        fn.published_at,
                        fn.url,
                        fn.news_source,
                        COALESCE(sa.sentiment_label, 'Not Analyzed') as sentiment_label,
                        COALESCE(sa.sentiment_score, 0.0) as sentiment_score,
                        COALESCE(sa.confidence_score, 0.0) as confidence_score,
                        COALESCE(sa.analysis_model, 'N/A') as analysis_model
                    FROM financial_news fn
                    LEFT JOIN sentiment_analysis sa ON fn.news_id = sa.news_id
                    ORDER BY fn.published_at DESC
                    LIMIT %s;
                """, (limit,))
            
            news_articles = cursor.fetchall()
            
            if not news_articles:
                print("\nNo news articles found in the specified time range.")
                return
            
            print(f"\nRecent News Articles ({len(news_articles)} displayed):")
            print("=" * 80)
            
            for i, (title, content, published_at, url, source, sentiment_label, 
                    sentiment_score, confidence_score, analysis_model) in enumerate(news_articles, 1):
                
                # Format published date
                pub_date = published_at.strftime('%Y-%m-%d %H:%M') if published_at else 'N/A'
                
                # Truncate content for display
                content_display = content[:200] + "..." if content and len(content) > 200 else (content or "No content available")
                
                print(f"\nArticle {i}")
                print(f"Title: {title}")
                print(f"Published: {pub_date}")
                print(f"Source: {source}")
                print(f"URL: {url}")
                print(f"Content: {content_display}")
                
                # Display sentiment analysis if available
                if sentiment_label != 'Not Analyzed':
                    sentiment_emoji = {"positive": "🟢", "negative": "🔴", "neutral": "🟡"}.get(sentiment_label.lower(), "⚪")
                    print(f"Sentiment: {sentiment_emoji} {sentiment_label.upper()} (Score: {sentiment_score:.3f}, Confidence: {confidence_score:.3f})")
                    print(f"Model: {analysis_model}")
                else:
                    print("Sentiment: Not Analyzed")
                
                print("-" * 80)
        
        except Exception as e:
            print(f"Error fetching news: {e}")
        
        finally:
            cursor.close()
            conn.close()
    
    def view_news_by_stock(self, stock_symbol, limit=10, days_back=30):
        """View news articles related to a specific stock."""
        conn = self.connect_to_database()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        print("=" * 80)
        print(f"NEWS FOR {stock_symbol.upper()}")
        print("=" * 80)
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Showing news from last {days_back} days (Limit: {limit})")
        
        try:
            # First check if stock exists
            cursor.execute('SELECT stock_id, company_name FROM stocks WHERE symbol = %s;', (stock_symbol.upper(),))
            stock = cursor.fetchone()
            
            if not stock:
                print(f"Stock {stock_symbol.upper()} not found in database.")
                print("Available stocks:")
                cursor.execute('SELECT symbol, company_name FROM stocks ORDER BY symbol LIMIT 10;')
                stocks = cursor.fetchall()
                for symbol, name in stocks:
                    print(f"   {symbol} - {name}")
                return
            
            stock_id, company_name = stock
            print(f"Company: {company_name}")
            
            # Get news related to this stock
            cursor.execute("""
                SELECT 
                    fn.title,
                    fn.content,
                    fn.published_at,
                    fn.url,
                    fn.news_source,
                    COALESCE(snr.relevance_score, 0.5) as relevance_score,
                    COALESCE(sa.sentiment_label, 'Not Analyzed') as sentiment_label,
                    COALESCE(sa.sentiment_score, 0.0) as sentiment_score,
                    COALESCE(sa.confidence_score, 0.0) as confidence_score,
                    COALESCE(sa.analysis_model, 'N/A') as analysis_model
                FROM financial_news fn
                JOIN stock_news_relations snr ON fn.news_id = snr.news_id
                LEFT JOIN sentiment_analysis sa ON fn.news_id = sa.news_id
                WHERE snr.stock_id = %s AND fn.published_at >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY snr.relevance_score DESC, fn.published_at DESC
                LIMIT %s;
            """, (stock_id, days_back, limit))
            
            news_articles = cursor.fetchall()
            
            if not news_articles:
                print(f"\nNo news articles found for {stock_symbol.upper()} in the specified time range.")
                return
            
            print(f"\nNews Articles for {stock_symbol.upper()} ({len(news_articles)} displayed):")
            print("=" * 80)
            
            for i, (title, content, published_at, url, source, relevance_score,
                    sentiment_label, sentiment_score, confidence_score, analysis_model) in enumerate(news_articles, 1):
                
                # Format published date
                pub_date = published_at.strftime('%Y-%m-%d %H:%M') if published_at else 'N/A'
                
                # Truncate content for display
                content_display = content[:200] + "..." if content and len(content) > 200 else (content or "No content available")
                
                print(f"\nArticle {i}")
                print(f"Title: {title}")
                print(f"Published: {pub_date}")
                print(f"Source: {source}")
                print(f"Relevance Score: {relevance_score:.2f}")
                print(f"URL: {url}")
                print(f"Content: {content_display}")
                
                # Display sentiment analysis if available
                if sentiment_label != 'Not Analyzed':
                    sentiment_emoji = {"positive": "🟢", "negative": "🔴", "neutral": "🟡"}.get(sentiment_label.lower(), "⚪")
                    print(f"Sentiment: {sentiment_emoji} {sentiment_label.upper()} (Score: {sentiment_score:.3f}, Confidence: {confidence_score:.3f})")
                    print(f"Model: {analysis_model}")
                else:
                    print("Sentiment: Not Analyzed")
                
                print("-" * 80)
        
        except Exception as e:
            print(f"Error fetching stock news: {e}")
        
        finally:
            cursor.close()
            conn.close()
    
    def view_news_by_sentiment(self, sentiment_label='positive', limit=15, days_back=7):
        """View news articles filtered by sentiment."""
        conn = self.connect_to_database()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        print("=" * 80)
        print(f"{sentiment_label.upper()} SENTIMENT NEWS")
        print("=" * 80)
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Showing news from last {days_back} days (Limit: {limit})")
        
        try:
            # Try with date filtering first, fallback to all sentiment news if no results
            try:
                cursor.execute("""
                    SELECT 
                        fn.title,
                        fn.content,
                        fn.published_at,
                        fn.url,
                        fn.news_source,
                        sa.sentiment_score,
                        sa.confidence_score,
                        sa.analysis_model,
                        array_agg(DISTINCT s.symbol) as related_stocks
                    FROM financial_news fn
                    JOIN sentiment_analysis sa ON fn.news_id = sa.news_id
                    LEFT JOIN stock_news_relations snr ON fn.news_id = snr.news_id
                    LEFT JOIN stocks s ON snr.stock_id = s.stock_id
                    WHERE sa.sentiment_label = %s AND fn.published_at >= CURRENT_DATE - INTERVAL '%s days'
                    GROUP BY fn.news_id, fn.title, fn.content, fn.published_at, fn.url, 
                             fn.news_source, sa.sentiment_score, sa.confidence_score, sa.analysis_model
                    ORDER BY sa.sentiment_score DESC, fn.published_at DESC
                    LIMIT %s;
                """, (sentiment_label.lower(), days_back, limit))
            except Exception as e:
                print(f"Date filtering failed: {e}, showing all {sentiment_label} news...")
                cursor.execute("""
                    SELECT 
                        fn.title,
                        fn.content,
                        fn.published_at,
                        fn.url,
                        fn.news_source,
                        sa.sentiment_score,
                        sa.confidence_score,
                        sa.analysis_model,
                        array_agg(DISTINCT s.symbol) as related_stocks
                    FROM financial_news fn
                    JOIN sentiment_analysis sa ON fn.news_id = sa.news_id
                    LEFT JOIN stock_news_relations snr ON fn.news_id = snr.news_id
                    LEFT JOIN stocks s ON snr.stock_id = s.stock_id
                    WHERE sa.sentiment_label = %s
                    GROUP BY fn.news_id, fn.title, fn.content, fn.published_at, fn.url, 
                             fn.news_source, sa.sentiment_score, sa.confidence_score, sa.analysis_model
                    ORDER BY sa.sentiment_score DESC, fn.published_at DESC
                    LIMIT %s;
                """, (sentiment_label.lower(), limit))
            
            news_articles = cursor.fetchall()
            
            if not news_articles:
                print(f"\nNo {sentiment_label} news articles found in the specified time range.")
                return
            
            print(f"\n{sentiment_label.title()} News Articles ({len(news_articles)} displayed):")
            print("=" * 80)
            
            for i, (title, content, published_at, url, source, sentiment_score,
                    confidence_score, analysis_model, related_stocks) in enumerate(news_articles, 1):
                
                # Format published date
                pub_date = published_at.strftime('%Y-%m-%d %H:%M') if published_at else 'N/A'
                
                # Truncate content for display
                content_display = content[:200] + "..." if content and len(content) > 200 else (content or "No content available")
                
                # Format related stocks
                stocks_display = ", ".join(filter(None, related_stocks)) if related_stocks else "None"
                
                print(f"\nArticle {i}")
                print(f"Title: {title}")
                print(f"Published: {pub_date}")
                print(f"Source: {source}")
                print(f"URL: {url}")
                print(f"Related Stocks: {stocks_display}")
                print(f"Content: {content_display}")
                print(f"Sentiment Score: {sentiment_score:.3f} (Confidence: {confidence_score:.3f})")
                print(f"Model: {analysis_model}")
                
                print("-" * 80)
        
        except Exception as e:
            print(f"Error fetching sentiment news: {e}")
        
        finally:
            cursor.close()
            conn.close()
    
    def view_news_summary(self):
        """View a summary of news statistics."""
        conn = self.connect_to_database()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        print("=" * 80)
        print("NEWS ANALYSIS SUMMARY")
        print("=" * 80)
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Total news count
            cursor.execute('SELECT COUNT(*) FROM financial_news;')
            total_news = cursor.fetchone()[0]
            print(f"Total news articles: {total_news:,}")
            
            # News sources
            cursor.execute("""
                SELECT COALESCE(fn.news_source, 'Unknown') as source_name, COUNT(fn.news_id) as article_count
                FROM financial_news fn
                GROUP BY COALESCE(fn.news_source, 'Unknown')
                ORDER BY article_count DESC;
            """)
            sources = cursor.fetchall()
            print(f"\nNews Sources ({len(sources)}):")
            for source_name, count in sources:
                print(f"   {source_name}: {count:,} articles")
            
            # Sentiment analysis summary
            cursor.execute("""
                SELECT 
                    sentiment_label,
                    COUNT(*) as count,
                    AVG(sentiment_score) as avg_score,
                    AVG(confidence_score) as avg_confidence
                FROM sentiment_analysis
                GROUP BY sentiment_label
                ORDER BY count DESC;
            """)
            sentiments = cursor.fetchall()
            print(f"\nSentiment Analysis Summary:")
            for label, count, avg_score, avg_confidence in sentiments:
                emoji = {"positive": "🟢", "negative": "🔴", "neutral": "🟡"}.get(label.lower(), "⚪")
                print(f"   {emoji} {label.title()}: {count:,} articles (Avg Score: {avg_score:.3f}, Avg Confidence: {avg_confidence:.3f})")
            
            # Most recent news by date
            cursor.execute("""
                SELECT DATE(published_at) as news_date, COUNT(*) as count
                FROM financial_news
                WHERE published_at >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(published_at)
                ORDER BY news_date DESC;
            """)
            daily_counts = cursor.fetchall()
            print(f"\nNews Volume (Last 7 Days):")
            for news_date, count in daily_counts:
                date_str = news_date.strftime('%Y-%m-%d') if news_date else 'Unknown'
                print(f"   {date_str}: {count:,} articles")
            
            # Top stocks with most news
            cursor.execute("""
                SELECT s.symbol, s.company_name, COUNT(snr.news_id) as news_count
                FROM stocks s
                JOIN stock_news_relations snr ON s.stock_id = snr.stock_id
                GROUP BY s.stock_id, s.symbol, s.company_name
                ORDER BY news_count DESC
                LIMIT 10;
            """)
            top_stocks = cursor.fetchall()
            print(f"\nTop 10 Stocks by News Volume:")
            for symbol, company_name, count in top_stocks:
                print(f"   {symbol}: {count:,} articles - {company_name}")
        
        except Exception as e:
            print(f"Error generating news summary: {e}")
        
        finally:
            cursor.close()
            conn.close()
    
    def export_news_to_dataframe(self, stock_symbol=None, days_back=7, sentiment_filter=None):
        """Export news to pandas DataFrame for further analysis."""
        session = self.db_manager.get_session()
        
        try:
            from database_models import FinancialNews, SentimentAnalysis, StockNewsRelation, Stock
            
            query = session.query(
                FinancialNews.title,
                FinancialNews.content,
                FinancialNews.published_at,
                FinancialNews.url,
                FinancialNews.news_source,
                SentimentAnalysis.sentiment_label,
                SentimentAnalysis.sentiment_score,
                SentimentAnalysis.confidence_score,
                SentimentAnalysis.analysis_model,
                Stock.symbol
            ).outerjoin(
                SentimentAnalysis, FinancialNews.news_id == SentimentAnalysis.news_id
            ).outerjoin(
                StockNewsRelation, FinancialNews.news_id == StockNewsRelation.news_id
            ).outerjoin(
                Stock, StockNewsRelation.stock_id == Stock.stock_id
            ).filter(
                FinancialNews.published_at >= (datetime.now() - timedelta(days=days_back))
            )
            
            if stock_symbol:
                query = query.filter(Stock.symbol == stock_symbol.upper())
            
            if sentiment_filter:
                query = query.filter(SentimentAnalysis.sentiment_label == sentiment_filter.lower())
            
            query = query.order_by(FinancialNews.published_at.desc())
            
            results = query.all()
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=[
                'title', 'content', 'published_at', 'url', 'source_name',
                'sentiment_label', 'sentiment_score', 'confidence_score',
                'analysis_model', 'symbol'
            ])
            
            return df
        
        except Exception as e:
            print(f"Error exporting news to DataFrame: {e}")
            return None
        
        finally:
            session.close()

def main():
    """Main function for interactive news viewing."""
    viewer = NewsViewer()
    
    print("Stock Tracker News Viewer")
    print("=" * 40)
    print("Choose an option:")
    print("1. View news for specific stock")
    print("2. View news summary and statistics")
    print("3. Export news to DataFrame")
    print("4. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            symbol = input("Enter stock symbol (e.g., AAPL): ").strip()
            if symbol:
                limit = input("Enter number of articles to show (default 10): ").strip()
                days = input("Enter days back (default 30): ").strip()
                limit = int(limit) if limit.isdigit() else 10
                days = int(days) if days.isdigit() else 30
                viewer.view_news_by_stock(symbol, limit=limit, days_back=days)
            
        elif choice == '2':
            viewer.view_news_summary()
            
        elif choice == '3':
            symbol = input("Enter stock symbol (optional, press Enter for all): ").strip()
            days = input("Enter days back (default 7): ").strip()
            sentiment = input("Enter sentiment filter (optional, positive/negative/neutral): ").strip()
            
            days = int(days) if days.isdigit() else 7
            sentiment = sentiment if sentiment in ['positive', 'negative', 'neutral'] else None
            symbol = symbol if symbol else None
            
            df = viewer.export_news_to_dataframe(stock_symbol=symbol, days_back=days, sentiment_filter=sentiment)
            
            if df is not None and not df.empty:
                print(f"\nExported {len(df)} news articles to DataFrame")
                print("Columns:", df.columns.tolist())
                print("\nFirst 5 rows:")
                print(df.head())
                
                save = input("\nSave to CSV? (y/n): ").strip().lower()
                if save == 'y':
                    filename = f"news_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved to {filename}")
            else:
                print("No data found for the specified criteria")
                
        elif choice == '4':
            print("Goodbye!")
            break
            
        else:
            print("Invalid choice. Please enter 1-4.")

if __name__ == "__main__":
    main()
