#!/usr/bin/env python3
"""
Utility script to retroactively link existing news articles to stocks.
Run this once to link any news articles that were stored before the linking fix.
"""

import logging
from database_models import db_manager
from etl_pipeline import ETLPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def main():
    """Link existing news articles to stocks."""
    logger.info("=" * 60)
    logger.info("LINKING EXISTING NEWS ARTICLES TO STOCKS")
    logger.info("=" * 60)
    
    try:
        etl = ETLPipeline()
        etl.link_existing_news_to_stocks()
        logger.info("Successfully linked existing news articles!")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1
    finally:
        if 'etl' in locals():
            etl.close()
    
    return 0

if __name__ == "__main__":
    exit(main())

