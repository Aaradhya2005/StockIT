#!/usr/bin/env python3
"""
Simple script to fetch and store news for tracked stocks.
This will fetch news for all stocks in the database and link them properly.
"""

import logging
import time
from database_models import db_manager
from etl_pipeline import ETLPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def main():
    """Fetch news for all tracked stocks."""
    logger.info("=" * 60)
    logger.info("FETCHING NEWS FOR TRACKED STOCKS")
    logger.info("=" * 60)
    
    try:
        etl = ETLPipeline()
        
        # Fetch news for all tracked stocks
        logger.info("Fetching news for all tracked stocks...")
        etl.run_news_etl(run_for_all_stocks=True)
        
        logger.info("Successfully fetched and stored news!")
        logger.info("You can now view the news in view_database.py")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if 'etl' in locals():
            etl.close()
    
    return 0

if __name__ == "__main__":
    exit(main())

