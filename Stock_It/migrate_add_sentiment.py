import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

host = os.getenv('DB_HOST','localhost')
port = os.getenv('DB_PORT','5432')
user = os.getenv('DB_USER','postgres')
password = os.getenv('DB_PASSWORD')
dbname = os.getenv('DB_NAME','stock_tracker_db')

print(f"Connecting to {host}:{port} as {user} to migrate DB {dbname}")
try:
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("ALTER TABLE financial_news ADD COLUMN IF NOT EXISTS sentiment VARCHAR(20);")
    print("✅ Migration applied: added column financial_news.sentiment (if it didn't exist)")
    cur.close()
    conn.close()
except Exception as e:
    print(f"❌ Migration failed: {e}")
    raise
