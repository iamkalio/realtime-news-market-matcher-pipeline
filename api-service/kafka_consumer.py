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
        self.transactions: Deque[Dict] = deque(maxlen=max_size)
        self.fraud_alerts: Deque[Dict] = deque(maxlen=max_size)
        self.lock = threading.Lock()
    
    def add_transaction(self, transaction: Dict):
        with self.lock:
            self.transactions.append(transaction)
    
    def add_fraud_alert(self, alert: Dict):
        with self.lock:
            self.fraud_alerts.append(alert)
    
    def get_recent_transactions(self, limit: int = 100) -> List[Dict]:
        with self.lock:
            return list(self.transactions)[-limit:]
    
    def get_recent_alerts(self, limit: int = 100) -> List[Dict]:
        with self.lock:
            return list(self.fraud_alerts)[-limit:]
    
    def get_stats(self) -> Dict:
        with self.lock:
            total_tx = len(self.transactions)
            total_alerts = len(self.fraud_alerts)
            fraud_rate = (total_alerts / total_tx * 100) if total_tx > 0 else 0.0
            
            latest_tx_time = self.transactions[-1]["timestamp"] if self.transactions else None
            latest_alert_time = self.fraud_alerts[-1]["timestamp"] if self.fraud_alerts else None
            
            return {
                "total_transactions": total_tx,
                "total_fraud_alerts": total_alerts,
                "fraud_rate": round(fraud_rate, 2),
                "latest_transaction_time": latest_tx_time,
                "latest_alert_time": latest_alert_time,
            }


def start_kafka_consumers(store: KafkaDataStore):
    """Start background threads to consume from Kafka topics."""
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    def consume_transactions():
        import time
        while True:
            try:
                consumer = KafkaConsumer(
                    "transactions",
                    bootstrap_servers=bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",  # Only new messages
                    consumer_timeout_ms=1000,
                )
                
                print("Started consuming transactions topic...")
                for message in consumer:
                    store.add_transaction(message.value)
                    print(f"Received transaction: {message.value['transaction_id']}")
            except Exception as e:
                print(f"Error consuming transactions: {e}, retrying in 5 seconds...")
                time.sleep(5)
    
    def consume_fraud_alerts():
        import time
        while True:
            try:
                consumer = KafkaConsumer(
                    "fraud-alerts",
                    bootstrap_servers=bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    consumer_timeout_ms=1000,
                )
                
                print("Started consuming fraud-alerts topic...")
                for message in consumer:
                    store.add_fraud_alert(message.value)
                    print(f"Received fraud alert: {message.value['transaction_id']}")
            except Exception as e:
                print(f"Error consuming fraud-alerts: {e}, retrying in 5 seconds...")
                time.sleep(5)
    
    # Start consumer threads
    tx_thread = threading.Thread(target=consume_transactions, daemon=True)
    alert_thread = threading.Thread(target=consume_fraud_alerts, daemon=True)
    
    tx_thread.start()
    alert_thread.start()
    
    return tx_thread, alert_thread

