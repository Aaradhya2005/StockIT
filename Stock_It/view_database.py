#!/usr/bin/env python3
"""Database Viewer Script"""

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

def connect_to_database():
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

def view_database_summary():
    conn = connect_to_database()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    print("=" * 60)
    print("📊 STOCK TRACKER DATABASE SUMMARY")
    print("=" * 60)
    print(f"🕐 Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print(f"\nAvailable tables ({len(tables)}):")
        for table in tables:
            print(f"   - {table[0]}")
        
        print("\n" + "="*50)
        print("STOCKS DATA")
        print("="*50)
        cursor.execute('SELECT COUNT(*) FROM stocks;')
        stock_count = cursor.fetchone()[0]
        print(f"Total companies tracked: {stock_count}")
        
        if stock_count > 0:
            cursor.execute('SELECT symbol, company_name, sector FROM stocks ORDER BY symbol LIMIT 20;')
            stocks = cursor.fetchall()
            print("\nCompanies in database:")
            for symbol, name, sector in stocks:
                sector_display = sector if sector else "N/A"
                print(f"   {symbol:6} | {name[:40]:40} | {sector_display}")
        
        print("\n" + "="*50)
        print("STOCK PRICES DATA")
        print("="*50)
        cursor.execute('SELECT COUNT(*) FROM stock_prices;')
        price_count = cursor.fetchone()[0]
        print(f"Total price records: {price_count:,}")
        
        if price_count > 0:
            cursor.execute("""
                SELECT s.symbol, sp.date, sp.close_price, sp.volume 
                FROM stock_prices sp 
                JOIN stocks s ON sp.stock_id = s.stock_id 
                ORDER BY sp.date DESC, s.symbol
                LIMIT 10;
            """)
            prices = cursor.fetchall()
            print("\nLatest price data:")
            print("   Symbol | Date       | Close Price | Volume")
            print("   " + "-"*45)
            for symbol, date, price, volume in prices:
                print(f"   {symbol:6} | {date} | ${float(price):8.2f} | {volume:,}")
        
        print("\n" + "="*50)
        print("FINANCIAL NEWS DATA")
        print("="*50)
        cursor.execute('SELECT COUNT(*) FROM financial_news;')
        news_count = cursor.fetchone()[0]
        print(f"Total news articles: {news_count:,}")
        
        if news_count > 0:
            cursor.execute("""
                SELECT fn.title, fn.published_at, ns.source_name 
                FROM financial_news fn 
                JOIN news_sources ns ON fn.source_id = ns.source_id 
                ORDER BY fn.published_at DESC 
                LIMIT 5;
            """)
            news = cursor.fetchall()
            print("\nLatest news articles:")
            for title, published_at, source in news:
                print(f"   {title[:60]}...")
                print(f"    Source: {source} | Date: {published_at}")
        
        print("\n" + "="*50)
        print("SENTIMENT ANALYSIS DATA")
        print("="*50)
        cursor.execute('SELECT COUNT(*) FROM sentiment_analysis;')
        sentiment_count = cursor.fetchone()[0]
        print(f"Total sentiment analysis records: {sentiment_count:,}")

        if sentiment_count > 0:
            cursor.execute("""
                SELECT sa.analysis_model, AVG(sa.sentiment_score), sa.sentiment_label
                FROM sentiment_analysis sa
                GROUP BY sa.analysis_model, sa.sentiment_label
                ORDER BY sa.analysis_model, sa.sentiment_label;
            """)
            sentiments = cursor.fetchall()
            print("\nSentiment analysis summary:")
            for model, avg_score, label in sentiments:
                print(f"   Model: {model}, Label: {label}, Avg Score: {avg_score:.2f}")

    except Exception as e:
        print(f"Error viewing database: {e}")
    
    finally:
        cursor.close()
        conn.close()

def view_specific_stock(symbol):
    conn = connect_to_database()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    print(f"\nDETAILED VIEW FOR {symbol.upper()}")
    print("="*50)
    
    try:
        cursor.execute('SELECT * FROM stocks WHERE symbol = %s;', (symbol.upper(),))
        stock = cursor.fetchone()
        
        if not stock:
            print(f"Stock {symbol.upper()} not found in database")
            return
        
        stock_id = stock[0]
        print(f"Company: {stock[1]}")
        print(f"Symbol: {stock[2]}")
        print(f"Sector: {stock[3] if stock[3] else 'N/A'}")
        print(f"Market Cap: {stock[4] if stock[4] else 'N/A'}")
        print(f"Exchange: {stock[5] if stock[5] else 'N/A'}")
        
        cursor.execute("""
            SELECT date, open_price, high_price, low_price, close_price, volume 
            FROM stock_prices 
            WHERE stock_id = %s 
            ORDER BY date DESC 
            LIMIT 10;
        """, (stock_id,))
        
        prices = cursor.fetchall()
        if prices:
            print(f"\nRecent price history:")
            print("Date       | Open    | High    | Low     | Close   | Volume")
            print("-" * 65)
            for date, open_p, high, low, close, volume in prices:
                print(f"{date} | ${float(open_p):6.2f} | ${float(high):6.2f} | ${float(low):6.2f} | ${float(close):6.2f} | {volume:,}")

        # Fetch and display news for the specific stock (LEFT JOIN to show news even without sentiment)
        cursor.execute("""
            SELECT fn.title, fn.published_at, 
                   COALESCE(sa.sentiment_label, 'N/A') as sentiment_label, 
                   COALESCE(sa.sentiment_score, 0.0) as sentiment_score, 
                   COALESCE(sa.analysis_model, 'N/A') as analysis_model
            FROM financial_news fn
            JOIN stock_news_relations snr ON fn.news_id = snr.news_id
            LEFT JOIN sentiment_analysis sa ON fn.news_id = sa.news_id
            WHERE snr.stock_id = %s
            ORDER BY fn.published_at DESC
            LIMIT 10;
        """, (stock_id,))

        news_articles = cursor.fetchall()
        if news_articles:
            print("\nRecent News and Sentiment:")
            print("-" * 80)
            for title, published_at, label, score, model in news_articles:
                print(f"Title: {title[:70]}")
                if label != 'N/A':
                    print(f"  Published: {published_at} | Sentiment: {label} ({score:.2f}) | Model: {model}")
                else:
                    print(f"  Published: {published_at} | Sentiment: Not analyzed yet")
        else:
            print("\nNo news articles found for this stock.")

    except Exception as e:
        print(f"Error viewing stock data: {e}")
    
    finally:
        cursor.close()
        conn.close()

def view_news_sources():
    conn = connect_to_database()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    print("\nNEWS SOURCES")
    print("="*40)
    
    try:
        cursor.execute('SELECT source_name, source_url FROM news_sources ORDER BY source_name;')
        sources = cursor.fetchall()
        
        for source_name, source_url in sources:
            print(f" {source_name}")
            print(f"  URL: {source_url if source_url else 'N/A'}")
        
    except Exception as e:
        print(f"Error viewing news sources: {e}")
    
    finally:
        cursor.close()
        conn.close()

def main():
    print("Stock Tracker Database Viewer")
    print("Choose an option:")
    print("1. View database summary")
    print("2. View specific stock")
    print("3. View news sources")
    print("4. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            view_database_summary()
        elif choice == '2':
            symbol = input("Enter stock symbol (e.g., AAPL): ").strip()
            if symbol:
                view_specific_stock(symbol)
        elif choice == '3':
            view_news_sources()
        elif choice == '4':
            print("Exitting!")
            break
        else:
            print("Invalid choice. Please enter 1-4.")

if __name__ == "__main__":
    main()