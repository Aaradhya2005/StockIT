import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

host = os.getenv('DB_HOST','localhost')
port = os.getenv('DB_PORT','5432')
user = os.getenv('DB_USER','postgres')
password = os.getenv('DB_PASSWORD')
dbname = os.getenv('DB_NAME','stock_tracker_db')

try:
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM financial_news;')
    count = cur.fetchone()[0]
    print(f"financial_news rows: {count}")

    if count > 0:
        cur.execute('SELECT news_id, news_source, company, symbol, title, published_at, url FROM financial_news ORDER BY published_at DESC LIMIT 10;')
        rows = cur.fetchall()
        for r in rows:
            print(r)
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error querying financial_news: {e}")
    raise
