"""Real-time Stock Monitor"""

import asyncio
import websocket
import json
import threading
import time
from datetime import datetime
from typing import Dict, List, Callable
import logging
from sqlalchemy.orm import Session

from database_models import db_manager, Stock, StockTick
from etl_pipeline import ETLPipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealTimeMonitor:
    def __init__(self, etl_pipeline: ETLPipeline = None):
        self.etl_pipeline = etl_pipeline or ETLPipeline()
        self.session = db_manager.get_session()
        self.monitored_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        self.price_cache = {}
        self.change_threshold = 0.02
        self.callbacks = []
        self.is_monitoring = False
        self.monitor_thread = None
        logger.info("Real-time monitor initialized")
    
    def add_change_callback(self, callback: Callable):
        self.callbacks.append(callback)
    
    def calculate_price_change(self, symbol: str, current_price: float) -> Dict:
        if symbol not in self.price_cache:
            self.price_cache[symbol] = current_price
            return {'change_percent': 0.0, 'is_significant': False}
        
        previous_price = self.price_cache[symbol]
        change_percent = ((current_price - previous_price) / previous_price) * 100
        is_significant = abs(change_percent) >= (self.change_threshold * 100)
        
        return {
            'previous_price': previous_price,
            'current_price': current_price,
            'change_percent': change_percent,
            'is_significant': is_significant
        }
    
    def store_tick_data(self, symbol: str, price: float, volume: int = 0, 
                       bid_price: float = None, ask_price: float = None):
        try:
            stock = self.session.query(Stock).filter_by(symbol=symbol).first()
            if not stock:
                logger.warning(f"Stock {symbol} not found in database")
                return
            
            tick_id = f"{symbol}_{int(time.time() * 1000)}"
            tick_record = StockTick(
                stock_id=stock.stock_id,
                tick_id=tick_id,
                timestamp=datetime.now(),
                price=price,
                volume=volume,
                bid_price=bid_price,
                ask_price=ask_price
            )
            
            self.session.add(tick_record)
            self.session.commit()
            logger.debug(f"Stored tick data for {symbol}: ${price}")
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error storing tick data for {symbol}: {e}")
    
    def handle_price_update(self, symbol: str, price_data: Dict):
        try:
            current_price = float(price_data.get('price', 0))
            volume = int(price_data.get('volume', 0))
            
            change_info = self.calculate_price_change(symbol, current_price)
            
            self.store_tick_data(symbol, current_price, volume)
            
            if change_info['is_significant']:
                logger.info(f"Significant price change for {symbol}: {change_info['change_percent']:.2f}%")
                
                for callback in self.callbacks:
                    try:
                        callback(symbol, change_info)
                    except Exception as e:
                        logger.error(f"Error in callback for {symbol}: {e}")
                
                self.trigger_etl_update(symbol)
            
            self.price_cache[symbol] = current_price
            
        except Exception as e:
            logger.error(f"Error handling price update for {symbol}: {e}")
    
    def trigger_etl_update(self, symbol: str):
        try:
            logger.info(f"Triggering ETL update for {symbol}")
            
            def run_etl():
                try:
                    self.etl_pipeline.run_stock_etl(symbol)
                    logger.info(f"ETL update completed for {symbol}")
                except Exception as e:
                    logger.error(f"ETL update failed for {symbol}: {e}")
            
            etl_thread = threading.Thread(target=run_etl, daemon=True)
            etl_thread.start()
            
        except Exception as e:
            logger.error(f"Error triggering ETL update for {symbol}: {e}")
    
    def simulate_price_feed(self):
        import random
        
        while self.is_monitoring:
            for symbol in self.monitored_symbols:
                base_price = self.price_cache.get(symbol, 100.0)
                price_change = random.uniform(-0.05, 0.05)
                new_price = base_price * (1 + price_change)
                volume = random.randint(1000, 10000)
                
                price_data = {
                    'symbol': symbol,
                    'price': new_price,
                    'volume': volume,
                    'timestamp': datetime.now().isoformat()
                }
                
                self.handle_price_update(symbol, price_data)
            
            time.sleep(5)
    
    def connect_to_alpha_vantage_realtime(self):
        logger.info("Alpha Vantage real-time connection not implemented")
        logger.info("Using simulation mode instead")
        self.simulate_price_feed()
    
    def start_monitoring(self, use_simulation: bool = True):
        if self.is_monitoring:
            logger.warning("Monitor is already running")
            return
        
        self.is_monitoring = True
        self.initialize_price_cache()
        
        if use_simulation:
            self.monitor_thread = threading.Thread(target=self.simulate_price_feed, daemon=True)
        else:
            self.monitor_thread = threading.Thread(target=self.connect_to_alpha_vantage_realtime, daemon=True)
        
        self.monitor_thread.start()
        logger.info(f"Started monitoring {len(self.monitored_symbols)} symbols")
    
    def stop_monitoring(self):
        if not self.is_monitoring:
            logger.warning("Monitor is not running")
            return
        
        self.is_monitoring = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        logger.info("Stopped monitoring")
    
    def initialize_price_cache(self):
        try:
            for symbol in self.monitored_symbols:
                stock = self.session.query(Stock).filter_by(symbol=symbol).first()
                if stock:
                    latest_tick = self.session.query(StockTick).filter_by(
                        stock_id=stock.stock_id
                    ).order_by(StockTick.timestamp.desc()).first()
                    
                    if latest_tick:
                        self.price_cache[symbol] = latest_tick.price
                    else:
                        self.price_cache[symbol] = 100.0
                else:
                    self.price_cache[symbol] = 100.0
            
            logger.info(f"Initialized price cache for {len(self.price_cache)} symbols")
            
        except Exception as e:
            logger.error(f"Error initializing price cache: {e}")
    
    def get_monitoring_status(self) -> Dict:
        return {
            'is_monitoring': self.is_monitoring,
            'monitored_symbols': self.monitored_symbols,
            'change_threshold': self.change_threshold,
            'cached_prices': len(self.price_cache)
        }
    
    def set_change_threshold(self, threshold: float):
        self.change_threshold = threshold
        logger.info(f"Change threshold set to {threshold * 100}%")
    
    def add_symbol(self, symbol: str):
        if symbol not in self.monitored_symbols:
            self.monitored_symbols.append(symbol)
            logger.info(f"Added {symbol} to monitoring list")
    
    def remove_symbol(self, symbol: str):
        if symbol in self.monitored_symbols:
            self.monitored_symbols.remove(symbol)
            if symbol in self.price_cache:
                del self.price_cache[symbol]
            logger.info(f"Removed {symbol} from monitoring list")
    
    def close(self):
        self.stop_monitoring()
        if self.session:
            self.session.close()
        logger.info("Real-time monitor closed")

def price_change_alert(symbol: str, change_info: Dict):
    logger.info(f"ALERT: {symbol} changed by {change_info['change_percent']:.2f}%")

def significant_volume_callback(symbol: str, change_info: Dict):
    logger.info(f"High volume detected for {symbol}")

if __name__ == "__main__":
    etl_pipeline = ETLPipeline()
    monitor = RealTimeMonitor(etl_pipeline)
    
    monitor.add_change_callback(price_change_alert)
    monitor.set_change_threshold(0.01)
    
    try:
        db_manager.create_tables()
        monitor.start_monitoring(use_simulation=True)
        
        logger.info("Real-time monitor is running. Press Ctrl+C to stop.")
        while True:
            time.sleep(10)
            status = monitor.get_monitoring_status()
            logger.info(f"Monitoring status: {len(status['monitored_symbols'])} symbols")
    
    except KeyboardInterrupt:
        logger.info("Real-time monitor stopped by user")
    except Exception as e:
        logger.error(f"Real-time monitor error: {e}")
    finally:
        monitor.close()
        etl_pipeline.close()