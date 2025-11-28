-- Create tables for Stock Tracker Database
-- Run this after creating the database with create_database.sql

-- Core Tables

-- 1. stocks - Master table for stock information
CREATE TABLE stocks (
    stock_id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    market_cap BIGINT,
    exchange VARCHAR(50),
    currency VARCHAR(3) DEFAULT 'USD',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. stock_prices - Daily stock price data with OHLCV information
CREATE TABLE stock_prices (
    price_id SERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(stock_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open_price DECIMAL(12, 4) NOT NULL,
    high_price DECIMAL(12, 4) NOT NULL,
    low_price DECIMAL(12, 4) NOT NULL,
    close_price DECIMAL(12, 4) NOT NULL,
    adjusted_close DECIMAL(12, 4),
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

-- 3. financial_news - Financial news articles (flattened source info)
CREATE TABLE financial_news (
    news_id SERIAL PRIMARY KEY,
    news_source VARCHAR(255),
    company VARCHAR(255),
    symbol VARCHAR(10),
    sentiment VARCHAR(20),
    title VARCHAR(500) NOT NULL,
    content TEXT,
    author VARCHAR(255),
    published_at TIMESTAMP NOT NULL,
    url VARCHAR(1000) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. stock_news_relations - Many-to-many relationship between stocks and news
CREATE TABLE stock_news_relations (
    relation_id SERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(stock_id) ON DELETE CASCADE,
    news_id INTEGER NOT NULL REFERENCES financial_news(news_id) ON DELETE CASCADE,
    relevance_score DECIMAL(3, 2) DEFAULT 0.50,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, news_id)
);

-- 6. sentiment_analysis - Sentiment analysis results for news articles
CREATE TABLE sentiment_analysis (
    sentiment_id SERIAL PRIMARY KEY,
    news_id INTEGER NOT NULL REFERENCES financial_news(news_id) ON DELETE CASCADE,
    sentiment_score DECIMAL(5, 4) NOT NULL, -- Range: -1.0 to 1.0
    sentiment_label VARCHAR(20) NOT NULL, -- positive, negative, neutral
    confidence_score DECIMAL(5, 4) NOT NULL, -- Range: 0.0 to 1.0
    analysis_model VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(news_id, analysis_model)
);

-- Advanced Tables

-- 7. daily_stock_summary - Daily aggregated summary data
CREATE TABLE daily_stock_summary (
    summary_id SERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(stock_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    price_change DECIMAL(12, 4),
    price_change_percent DECIMAL(8, 4),
    news_count INTEGER DEFAULT 0,
    average_sentiment DECIMAL(5, 4),
    volume_summary BIGINT,
    high_low_spread DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

-- Real-Time Data Table

-- 8. stock_ticks - High-frequency trade ticks for real-time analytics
CREATE TABLE stock_ticks (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(stock_id) ON DELETE CASCADE,
    tick_id VARCHAR(100) NOT NULL, -- For deduplication
    timestamp TIMESTAMP NOT NULL,
    price DECIMAL(12, 4) NOT NULL,
    volume INTEGER NOT NULL,
    bid_price DECIMAL(12, 4),
    ask_price DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, tick_id)
);

-- Create indexes for better performance
CREATE INDEX idx_stock_prices_stock_date ON stock_prices(stock_id, date DESC);
CREATE INDEX idx_stock_prices_date ON stock_prices(date DESC);
CREATE INDEX idx_financial_news_published ON financial_news(published_at DESC);
CREATE INDEX idx_stock_news_relations_stock ON stock_news_relations(stock_id);
CREATE INDEX idx_stock_news_relations_news ON stock_news_relations(news_id);
CREATE INDEX idx_sentiment_analysis_news ON sentiment_analysis(news_id);
CREATE INDEX idx_stock_ticks_stock_timestamp ON stock_ticks(stock_id, timestamp DESC);
CREATE INDEX idx_stock_ticks_timestamp ON stock_ticks(timestamp DESC);
CREATE INDEX idx_daily_summary_stock_date ON daily_stock_summary(stock_id, date DESC);

-- Create triggers to update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_stocks_updated_at BEFORE UPDATE ON stocks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();