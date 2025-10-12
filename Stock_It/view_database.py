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
        print("STOCK TICKS DATA")
        print("="*50)
        cursor.execute('SELECT COUNT(*) FROM stock_ticks;')
        tick_count = cursor.fetchone()[0]
        print(f"Total tick records: {tick_count:,}")
        
        if tick_count > 0:
            cursor.execute("""
                SELECT s.symbol, st.timestamp, st.price, st.volume 
                FROM stock_ticks st 
                JOIN stocks s ON st.stock_id = s.stock_id 
                ORDER BY st.timestamp DESC 
                LIMIT 5;
            """)
            ticks = cursor.fetchall()
            print("\nLatest tick data:")
            for symbol, timestamp, price, volume in ticks:
                print(f"   {symbol:6} | {timestamp} | ${float(price):8.2f} | Vol: {volume:,}")
        
        print("\n" + "="*50)
        print("DATA FRESHNESS")
        print("="*50)
        
        cursor.execute("""
            SELECT MAX(date) as latest_price_date 
            FROM stock_prices;
        """)
        latest_price = cursor.fetchone()[0]
        if latest_price:
            print(f"Latest stock price data: {latest_price}")
        
        cursor.execute("""
            SELECT MAX(published_at) as latest_news_date 
            FROM financial_news;
        """)
        latest_news = cursor.fetchone()[0]
        if latest_news:
            print(f"Latest news article: {latest_news}")
        
        cursor.execute("""
            SELECT MAX(timestamp) as latest_tick_time 
            FROM stock_ticks;
        """)
        latest_tick = cursor.fetchone()[0]
        if latest_tick:
            print(f"Latest tick data: {latest_tick}")
        
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
        """, (stock[0],))
        
        prices = cursor.fetchall()
        if prices:
            print(f"\nRecent price history:")
            print("Date       | Open    | High    | Low     | Close   | Volume")
            print("-" * 65)
            for date, open_p, high, low, close, volume in prices:
                print(f"{date} | ${float(open_p):6.2f} | ${float(high):6.2f} | ${float(low):6.2f} | ${float(close):6.2f} | {volume:,}")
        
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
        cursor.execute('SELECT source_name, url FROM news_sources ORDER BY source_name;')
        sources = cursor.fetchall()
        
        for source_name, url in sources:
            print(f" {source_name}")
            print(f"  URL: {url}")
        
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