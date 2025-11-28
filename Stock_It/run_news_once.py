"""Run news ETL once and exit."""
from etl_pipeline import ETLPipeline
from database_models import db_manager

if __name__ == '__main__':
    # Ensure tables exist
    db_manager.create_tables()

    etl = ETLPipeline()
    try:
        print("Starting one-off news ETL (run_for_all_stocks=True)")
        etl.run_news_etl(run_for_all_stocks=True)
        print("News ETL completed")
    except Exception as e:
        print(f"Error running news ETL: {e}")
    finally:
        etl.close()
        db_manager.close()
