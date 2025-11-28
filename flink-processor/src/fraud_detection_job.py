import json
import os
import logging
from typing import Any, Dict, Optional

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer,
    FlinkKafkaProducer,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "transactions")
ALERTS_TOPIC = os.getenv("KAFKA_SINK_TOPIC", "fraud-alerts")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "5000"))

logger.info(f"Configuration:")
logger.info(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
logger.info(f"  Source Topic: {TRANSACTIONS_TOPIC}")
logger.info(f"  Sink Topic: {ALERTS_TOPIC}")
logger.info(f"  Fraud Threshold: {FRAUD_THRESHOLD}")


def _parse_transaction(value: str) -> Optional[Dict[str, Any]]:
    """Parse JSON transaction string to dictionary."""
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse transaction: {e}")
    return None


def main():
    """Main fraud detection pipeline."""
    logger.info("Starting Fraud Detection Pipeline")
    
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Enable logging
    logger.info("Environment created, configuring Kafka source...")

    # Kafka source with proper properties
    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "flink-fraud-detection",
        "auto.offset.reset": "earliest",
    }
    
    try:
        kafka_source = FlinkKafkaConsumer(
            topics=TRANSACTIONS_TOPIC,
            properties=kafka_props,
            deserialization_schema=SimpleStringSchema(),
        )
        logger.info("✓ Kafka source created successfully")
    except Exception as e:
        logger.error(f"✗ Failed to create Kafka source: {e}")
        raise

    transactions = env.add_source(kafka_source)
    logger.info("✓ Source added to environment")

    # Parse and filter transactions
    parsed_transactions = (
        transactions
        .map(_parse_transaction)
        .filter(lambda tx: tx is not None)
    )
    logger.info("✓ Parsing and filtering configured")

    # Fraud Detection Logic - filter by amount threshold
    fraud_alerts = parsed_transactions.filter(
        lambda tx: float(tx.get("amount", 0)) > FRAUD_THRESHOLD
    )
    logger.info(f"✓ Fraud detection filter configured (threshold: {FRAUD_THRESHOLD})")

    fraud_alert_strings = fraud_alerts.map(
        lambda tx: json.dumps(tx),
        output_type=Types.STRING(),
    )

    # Send to Kafka Sink
    try:
        kafka_sink = FlinkKafkaProducer(
            topic=ALERTS_TOPIC,
            producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            serialization_schema=SimpleStringSchema(),
        )
        logger.info("✓ Kafka sink created successfully")
    except Exception as e:
        logger.error(f"✗ Failed to create Kafka sink: {e}")
        raise

    fraud_alert_strings.add_sink(kafka_sink)
    logger.info("✓ Sink added to pipeline")

    logger.info("Executing job...")
    env.execute("Fraud Detection Job")


if __name__ == "__main__":
    main()
