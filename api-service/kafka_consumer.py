import json
import logging
import os
import threading
import time
from collections import deque
from typing import Callable, Deque, Dict, List

from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaDataStore:
    """Thread-safe in-memory store for Kafka messages."""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.news_stream: Deque[Dict] = deque(maxlen=max_size)
        self.processed_news: Deque[Dict] = deque(maxlen=max_size)
        self.lock = threading.Lock()
    
    def add_news(self, news: Dict) -> None:
        """Add a news item to the stream."""
        with self.lock:
            self.news_stream.append(news)
    
    def add_processed_news(self, processed: Dict) -> None:
        """Add a processed news item."""
        with self.lock:
            self.processed_news.append(processed)
    
    def get_recent_news(self, limit: int = 100) -> List[Dict]:
        """Get the most recent news items, up to the specified limit."""
        with self.lock:
            return list(self.news_stream)[-limit:]
    
    def get_recent_processed(self, limit: int = 100) -> List[Dict]:
        """Get the most recent processed news items, up to the specified limit."""
        with self.lock:
            return list(self.processed_news)[-limit:]
    
    def get_stats(self) -> Dict:
        """Get statistics about stored news items."""
        with self.lock:
            total_news = len(self.news_stream)
            total_processed = len(self.processed_news)
            processing_rate = (
                (total_processed / total_news * 100) if total_news > 0 else 0.0
            )
            
            latest_news_time = (
                self.news_stream[-1].get("time") if self.news_stream else None
            )
            latest_processed_time = (
                self.processed_news[-1].get("time") if self.processed_news else None
            )
            
            return {
                "total_news": total_news,
                "total_processed": total_processed,
                "processing_rate": round(processing_rate, 2),
                "latest_news_time": latest_news_time,
                "latest_processed_time": latest_processed_time,
            }


def _create_consumer(
    topic: str, bootstrap_servers: str
) -> KafkaConsumer:
    """Create and configure a Kafka consumer for the given topic."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


def _consume_topic(
    topic: str,
    bootstrap_servers: str,
    store_callback: Callable[[Dict], None],
    log_prefix: str,
) -> None:
    """Consume messages from a Kafka topic and store them using the callback.
    
    Args:
        topic: Kafka topic name to consume from
        bootstrap_servers: Kafka broker addresses
        store_callback: Function to call with each message value
        log_prefix: Prefix for log messages to identify the consumer
    """
    retry_delay = 5
    
    while True:
        consumer = None
        try:
            consumer = _create_consumer(topic, bootstrap_servers)
            logger.info(f"{log_prefix}: Started consuming from topic '{topic}'")
            
            for message in consumer:
                try:
                    store_callback(message.value)
                    logger.debug(
                        f"{log_prefix}: Received message from {message.value.get('source', 'Unknown')}"
                    )
                except Exception as e:
                    logger.error(f"{log_prefix}: Error processing message: {e}", exc_info=True)
                    
        except KafkaError as e:
            logger.error(
                f"{log_prefix}: Kafka error consuming from '{topic}': {e}, "
                f"retrying in {retry_delay} seconds...",
                exc_info=True
            )
        except Exception as e:
            logger.error(
                f"{log_prefix}: Unexpected error consuming from '{topic}': {e}, "
                f"retrying in {retry_delay} seconds...",
                exc_info=True
            )
        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
        
        time.sleep(retry_delay)


def start_kafka_consumers(store: KafkaDataStore) -> None:
    """Start background threads to consume from Kafka topics.
    
    Args:
        store: KafkaDataStore instance to store consumed messages
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    def consume_news_stream() -> None:
        _consume_topic(
            topic="news_stream",
                    bootstrap_servers=bootstrap_servers,
            store_callback=store.add_news,
            log_prefix="NewsStreamConsumer",
        )
    
    def consume_processed_news() -> None:
        _consume_topic(
            topic="processed_news",
                    bootstrap_servers=bootstrap_servers,
            store_callback=store.add_processed_news,
            log_prefix="ProcessedNewsConsumer",
        )
    
    news_thread = threading.Thread(target=consume_news_stream, daemon=True, name="news-consumer")
    processed_thread = threading.Thread(
        target=consume_processed_news, daemon=True, name="processed-consumer"
    )
    
    news_thread.start()
    processed_thread.start()
    
    logger.info("Kafka consumer threads started")
