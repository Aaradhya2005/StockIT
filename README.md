# StockIT - Continuous Stock Price Tracker

StockIT is a comprehensive stock tracking application that monitors the top 20 companies, fetches real-time stock data, analyzes market news sentiment, and stores everything in a database for further analysis.

## Features

- **Real-time Stock Tracking**: Monitors stock prices for top 20 major companies (AAPL, MSFT, GOOGL, etc.).
- **News Integration**: Fetches financial news from multiple sources (NewsAPI, etc.).
- **Sentiment Analysis**: Analyzes news sentiment using TextBlob and VADER.
- **ETL Pipeline**: Robust Extract, Transform, Load pipeline for data processing.
- **Database Integration**: Stores historical data and news in a SQL database.
- **Power BI Integration**: Includes a Power BI file (`stock_summary and news.pbix`) for visualization.

## Project Structure

- `Stock_It/`: Main application source code.
  - `main.py`: Entry point of the application.
  - `etl_pipeline.py`: Handles data extraction and transformation.
  - `database_models.py`: Database schema definitions.
  - `sentiment_analyzer.py`: Logic for analyzing news sentiment.
  - `config.py`: Configuration and API key validation.
- `stock_summary and news.pbix`: Power BI report for data visualization.

## Setup

1.  **Prerequisites**:
    - Python 3.8+
    - PostgreSQL (or configured database)

2.  **Installation**:
    ```bash
    cd Stock_It
    pip install -r requirements.txt
    ```

3.  **Configuration**:
    - Create a `.env` file in the `Stock_It` directory with your API keys (Alpha Vantage, NewsAPI, Database URL).

## Usage

To start the continuous tracker:

```bash
cd Stock_It
python main.py
```

The application will start monitoring stocks and news, updating the database at configured intervals.
