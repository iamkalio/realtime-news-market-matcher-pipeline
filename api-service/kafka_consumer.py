import json
import os
import threading
from collections import deque
from datetime import datetime
from typing import Deque, Dict, List

from kafka import KafkaConsumer


class KafkaDataStore:
    """Thread-safe in-memory store for Kafka messages."""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.news_stream: Deque[Dict] = deque(maxlen=max_size)
        self.processed_news: Deque[Dict] = deque(maxlen=max_size)
        self.lock = threading.Lock()
    
    def add_news(self, news: Dict):
        with self.lock:
            self.news_stream.append(news)
    
    def add_processed_news(self, processed: Dict):
        with self.lock:
            self.processed_news.append(processed)
    
    def get_recent_news(self, limit: int = 100) -> List[Dict]:
        with self.lock:
            return list(self.news_stream)[-limit:]
    
    def get_recent_processed(self, limit: int = 100) -> List[Dict]:
        with self.lock:
            return list(self.processed_news)[-limit:]
    
    def get_stats(self) -> Dict:
        with self.lock:
            total_news = len(self.news_stream)
            total_processed = len(self.processed_news)
            processing_rate = (total_processed / total_news * 100) if total_news > 0 else 0.0
            
            latest_news_time = self.news_stream[-1].get("time") if self.news_stream else None
            latest_processed_time = self.processed_news[-1].get("time") if self.processed_news else None
            
            return {
                "total_news": total_news,
                "total_processed": total_processed,
                "processing_rate": round(processing_rate, 2),
                "latest_news_time": latest_news_time,
                "latest_processed_time": latest_processed_time,
            }


def start_kafka_consumers(store: KafkaDataStore):
    """Start background threads to consume from Kafka topics."""
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    def consume_news_stream():
        import time
        while True:
            try:
                consumer = KafkaConsumer(
                    "news_stream",
                    bootstrap_servers=bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",  # Only new messages
                    consumer_timeout_ms=1000,
                )
                
                print("Started consuming news_stream topic...")
                for message in consumer:
                    store.add_news(message.value)
                    print(f"Received news: {message.value.get('source', 'Unknown')} - {message.value.get('news', '')[:50]}...")
            except Exception as e:
                print(f"Error consuming news_stream: {e}, retrying in 5 seconds...")
                time.sleep(5)
    
    def consume_processed_news():
        import time
        while True:
            try:
                consumer = KafkaConsumer(
                    "processed_news",
                    bootstrap_servers=bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    consumer_timeout_ms=1000,
                )
                
                print("Started consuming processed_news topic...")
                for message in consumer:
                    store.add_processed_news(message.value)
                    print(f"Received processed news: {message.value.get('source', 'Unknown')} - matched: {message.value.get('matched_market', 'None')}")
            except Exception as e:
                print(f"Error consuming processed_news: {e}, retrying in 5 seconds...")
                time.sleep(5)
    
    # Start consumer threads
    news_thread = threading.Thread(target=consume_news_stream, daemon=True)
    processed_thread = threading.Thread(target=consume_processed_news, daemon=True)
    
    news_thread.start()
    processed_thread.start()
    
    return news_thread, processed_thread

