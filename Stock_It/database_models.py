"""
SQLAlchemy ORM models for the Stock Tracker database.
"""

from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Boolean, DateTime, Date, Text, ForeignKey, UniqueConstraint, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class Stock(Base):
    __tablename__ = 'stocks'
    
    stock_id = Column(Integer, primary_key=True)
    symbol = Column(String(10), nullable=False, unique=True)
    company_name = Column(String(255), nullable=False)
    sector = Column(String(100))
    market_cap = Column(BigInteger)
    exchange = Column(String(50))
    currency = Column(String(3), default='USD')
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    stock_prices = relationship("StockPrice", back_populates="stock", cascade="all, delete-orphan")
    stock_news_relations = relationship("StockNewsRelation", back_populates="stock", cascade="all, delete-orphan")
    stock_predictions = relationship("StockPrediction", back_populates="stock", cascade="all, delete-orphan")
    daily_summaries = relationship("DailyStockSummary", back_populates="stock", cascade="all, delete-orphan")
    stock_ticks = relationship("StockTick", back_populates="stock", cascade="all, delete-orphan")

class StockPrice(Base):
    __tablename__ = 'stock_prices'
    
    price_id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stocks.stock_id'), nullable=False)
    date = Column(Date, nullable=False)
    open_price = Column(Numeric(10, 2), nullable=False)
    high_price = Column(Numeric(10, 2), nullable=False)
    low_price = Column(Numeric(10, 2), nullable=False)
    close_price = Column(Numeric(10, 2), nullable=False)
    adjusted_close = Column(Numeric(12, 4))
    volume = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, default=func.current_timestamp())
    
    __table_args__ = (UniqueConstraint('stock_id', 'date'),)
    
    # Relationships
    stock = relationship("Stock", back_populates="stock_prices")

class FinancialNews(Base):
    __tablename__ = 'financial_news'

    news_id = Column(Integer, primary_key=True)
    news_source = Column(String(255))
    company = Column(String(255))
    symbol = Column(String(10))
    sentiment = Column(String(20))
    title = Column(String(500), nullable=False)
    content = Column(Text)
    author = Column(String(255))
    published_at = Column(DateTime, nullable=False)
    url = Column(String(1000), unique=True)
    created_at = Column(DateTime, default=func.current_timestamp())

    # Relationships
    stock_news_relations = relationship("StockNewsRelation", back_populates="news", cascade="all, delete-orphan")
    sentiment_analysis = relationship("SentimentAnalysis", back_populates="news", cascade="all, delete-orphan")

class StockNewsRelation(Base):
    __tablename__ = 'stock_news_relations'
    
    relation_id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stocks.stock_id'), nullable=False)
    news_id = Column(Integer, ForeignKey('financial_news.news_id'), nullable=False)
    relevance_score = Column(Numeric(3, 2), default=0.50)
    created_at = Column(DateTime, default=func.current_timestamp())
    
    __table_args__ = (UniqueConstraint('stock_id', 'news_id'),)
    
    # Relationships
    stock = relationship("Stock", back_populates="stock_news_relations")
    news = relationship("FinancialNews", back_populates="stock_news_relations")

class SentimentAnalysis(Base):
    __tablename__ = 'sentiment_analysis'
    
    sentiment_id = Column(Integer, primary_key=True)
    news_id = Column(Integer, ForeignKey('financial_news.news_id'), nullable=False)
    sentiment_score = Column(Numeric(5, 4), nullable=False)  # Range: -1.0 to 1.0
    sentiment_label = Column(String(20), nullable=False)  # positive, negative, neutral
    confidence_score = Column(Numeric(5, 4), nullable=False)  # Range: 0.0 to 1.0
    analysis_model = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=func.current_timestamp())
    
    __table_args__ = (UniqueConstraint('news_id', 'analysis_model'),)
    
    # Relationships
    news = relationship("FinancialNews", back_populates="sentiment_analysis")

class StockPrediction(Base):
    __tablename__ = 'stock_predictions'
    
    prediction_id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stocks.stock_id'), nullable=False)
    prediction_date = Column(Date, nullable=False)
    target_date = Column(Date, nullable=False)
    predicted_price = Column(Numeric(12, 4), nullable=False)
    actual_price = Column(Numeric(12, 4))
    confidence_interval_lower = Column(Numeric(12, 4))
    confidence_interval_upper = Column(Numeric(12, 4))
    model_name = Column(String(100), nullable=False)
    model_version = Column(String(50))
    created_at = Column(DateTime, default=func.current_timestamp())
    
    # Relationships
    stock = relationship("Stock", back_populates="stock_predictions")

class DailyStockSummary(Base):
    __tablename__ = 'daily_stock_summary'
    
    summary_id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stocks.stock_id'), nullable=False)
    date = Column(Date, nullable=False)
    price_change = Column(Numeric(12, 4))
    price_change_percent = Column(Numeric(8, 4))
    news_count = Column(Integer, default=0)
    average_sentiment = Column(Numeric(5, 4))
    volume_summary = Column(BigInteger)
    high_low_spread = Column(Numeric(12, 4))
    created_at = Column(DateTime, default=func.current_timestamp())
    
    __table_args__ = (UniqueConstraint('stock_id', 'date'),)
    
    # Relationships
    stock = relationship("Stock", back_populates="daily_summaries")

class StockTick(Base):
    __tablename__ = 'stock_ticks'
    
    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stocks.stock_id'), nullable=False)
    tick_id = Column(String(100), nullable=False)  # For deduplication
    timestamp = Column(DateTime, nullable=False)
    price = Column(Numeric(12, 4), nullable=False)
    volume = Column(Integer, nullable=False)
    bid_price = Column(Numeric(12, 4))
    ask_price = Column(Numeric(12, 4))
    created_at = Column(DateTime, default=func.current_timestamp())
    
    __table_args__ = (UniqueConstraint('stock_id', 'tick_id'),)
    
    # Relationships
    stock = relationship("Stock", back_populates="stock_ticks")

class DatabaseManager:
    """Database connection and session management."""
    
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self.connect()
    
    def connect(self):
        """Create database connection."""
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'stock_tracker_db')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', '')
        
        # URL encode the password to handle special characters
        from urllib.parse import quote_plus
        encoded_password = quote_plus(db_password) if db_password else ''
        
        database_url = f"postgresql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"
        
        try:
            self.engine = create_engine(database_url, echo=False)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            print(f"Connected to database: {db_name}")
        except Exception as e:
            print(f"Database connection failed: {e}")
            raise
    
    def create_tables(self):
        """Create all tables in the database."""
        try:
            Base.metadata.create_all(bind=self.engine)
            print("All tables created successfully")
        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            raise
    
    def get_session(self):
        """Get a database session."""
        return self.SessionLocal()
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            print("✅ Database connection closed")

# Global database manager instance
db_manager = DatabaseManager()

def get_db_session():
    """Get a database session with automatic cleanup."""
    session = db_manager.get_session()
    try:
        yield session
    finally:
        session.close()