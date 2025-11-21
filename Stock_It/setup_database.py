#!/usr/bin/env python3
"""
Database Setup Script
Sets up the PostgreSQL database and tables for the stock tracker
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

def setup_database():
    """Setup the database and tables"""
    load_dotenv()
    
    # First, try to connect to PostgreSQL server (not specific database)
    try:
        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD')
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        db_name = os.getenv('DB_NAME', 'stock_tracker_db')
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f'CREATE DATABASE {db_name}')
            print(f"Created database: {db_name}")
        else:
            print(f"Database {db_name} already exists")
        
        cursor.close()
        conn.close()
        
        # Now connect to the specific database and create tables
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=db_name,
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD')
        )
        
        cursor = conn.cursor()
        
        # Read and execute the create_tables.sql file
        with open('sql/create_tables.sql', 'r') as f:
            sql_commands = f.read()
        
        # Execute the SQL commands
        cursor.execute(sql_commands)
        conn.commit()
        
        print("✅ Database tables created successfully!")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        if "password authentication failed" in str(e):
            print("❌ Password authentication failed!")
            print("\n🔧 Please update your .env file with the correct PostgreSQL password:")
            print("   DB_PASSWORD=your_actual_postgres_password")
        elif "could not connect to server" in str(e):
            print("❌ Could not connect to PostgreSQL server!")
            print("\n🔧 Please make sure PostgreSQL is installed and running:")
            print("   - Install PostgreSQL from https://www.postgresql.org/download/")
            print("   - Start the PostgreSQL service")
            print("   - Update your .env file with correct credentials")
        else:
            print(f"❌ Database connection error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Setting up PostgreSQL database...")
    success = setup_database()
    
    if success:
        print("\n🎉 Database setup complete!")
        print("You can now run: python view_database.py")
    else:
        print("\n❌ Database setup failed. Please check the error messages above.")