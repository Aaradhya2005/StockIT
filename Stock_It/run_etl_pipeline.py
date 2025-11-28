#!/usr/bin/env python3
"""
Real-Time Stock ETL Pipeline Runner

This script demonstrates how to run the complete ETL pipeline with real-time monitoring.
It sets up the database, initializes the ETL pipeline, and starts real-time monitoring.
"""

import os
import sys
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

# Import our custom modules
from database_models import DatabaseManager
from etl_pipeline import ETLPipeline
from real_time_monitor import RealTimeMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def setup_environment():
    """Load environment variables and validate configuration."""
    load_dotenv()

    # Only database credentials are required now (Yahoo Finance doesn't need API key)
    required_vars = [
        'DB_HOST',
        'DB_PORT',
        'DB_NAME',
        'DB_USER',
        'DB_PASSWORD'
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return False

    logger.info("Environment configuration validated successfully")
    return True

def initialize_database():
    """Initialize database connection and create tables if needed."""
    try:
        db_manager = DatabaseManager()
        
        # Test database connection
        with db_manager.get_session() as session:
            logger.info("Database connection established successfully")
            
        # Note: Tables should be created using create_tables.sql first
        logger.info("Database initialization completed")
        return db_manager
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return None

def run_initial_data_load(etl_pipeline, symbols):
    """Run initial data load for specified stock symbols."""
    logger.info("Starting initial data load...")

    try:
        # Load historical stock data using the correct method
        for symbol in symbols:
            logger.info(f"Loading historical data for {symbol}")
            etl_pipeline.run_stock_etl(symbol)
            time.sleep(12)  # Rate limiting for Yahoo Finance

        # Load market news for all tracked stocks
        logger.info("Loading market news for all tracked stocks")
        etl_pipeline.run_news_etl(run_for_all_stocks=True)

        logger.info("Initial data load completed successfully")
        return True

    except Exception as e:
        logger.error(f"Initial data load failed: {e}")
        return False

def start_real_time_monitoring(monitor, symbols):
    """Start real-time monitoring for specified symbols."""
    logger.info("Starting real-time monitoring...")
    
    try:
        # Add symbols to monitor
        for symbol in symbols:
            monitor.add_symbol(symbol)
            logger.info(f"Added {symbol} to monitoring")
        
        # Start monitoring (this will run in a separate thread)
        monitor.start_monitoring()
        logger.info("Real-time monitoring started successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to start real-time monitoring: {e}")
        return False

def main():
    """Main function to run the ETL pipeline."""
    logger.info("=" * 60)
    logger.info("STARTING REAL-TIME STOCK ETL PIPELINE")
    logger.info("=" * 60)
    
    # Step 1: Setup environment
    if not setup_environment():
        logger.error("Environment setup failed. Exiting.")
        return 1
    
    # Step 2: Initialize database
    db_manager = initialize_database()
    if not db_manager:
        logger.error("Database initialization failed. Exiting.")
        return 1
    
    # Step 3: Initialize ETL pipeline
    try:
        etl_pipeline = ETLPipeline()
        logger.info("ETL Pipeline initialized successfully")
    except Exception as e:
        logger.error(f"ETL Pipeline initialization failed: {e}")
        return 1
    
    # Step 4: Initialize real-time monitor
    try:
        monitor = RealTimeMonitor(etl_pipeline)
        logger.info("Real-time monitor initialized successfully")
    except Exception as e:
        logger.error(f"Real-time monitor initialization failed: {e}")
        return 1
    
    # Step 5: Define symbols to track
    symbols_to_track = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
    logger.info(f"Tracking symbols: {symbols_to_track}")
    
    # Step 6: Run initial data load
    if not run_initial_data_load(etl_pipeline, symbols_to_track):
        logger.error("Initial data load failed. Continuing with real-time monitoring...")
    
    # Step 7: Start real-time monitoring
    if not start_real_time_monitoring(monitor, symbols_to_track):
        logger.error("Real-time monitoring failed to start. Exiting.")
        return 1
    
    # Step 8: Schedule periodic jobs
    try:
        etl_pipeline.schedule_etl_jobs()
        logger.info("Periodic jobs scheduled successfully")
    except Exception as e:
        logger.error(f"Failed to schedule periodic jobs: {e}")
    
    # Step 9: Keep the pipeline running
    logger.info("ETL Pipeline is now running. Press Ctrl+C to stop.")
    try:
        import schedule
        while True:
            # Run scheduled jobs
            schedule.run_pending()
            
            # Display monitoring status every 5 minutes
            if int(time.time()) % 300 == 0:
                status = monitor.get_monitoring_status()
                logger.info(f"Monitoring Status: {status}")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Shutting down...")
        
        # Stop monitoring
        monitor.stop_monitoring()
        logger.info("Real-time monitoring stopped")
        
        # Close database connections
        db_manager.close()
        logger.info("Database connections closed")
        
        logger.info("ETL Pipeline shutdown completed")
        return 0
    
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)