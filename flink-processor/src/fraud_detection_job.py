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

# Updated topics for news processing
NEWS_SOURCE_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "news_stream")
PROCESSED_NEWS_TOPIC = os.getenv("KAFKA_SINK_TOPIC", "processed_news")

logger.info("Configuration:")
logger.info(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
logger.info(f"  Source Topic: {NEWS_SOURCE_TOPIC}")
logger.info(f"  Sink Topic: {PROCESSED_NEWS_TOPIC}")


# -------------------------------
# JSON Parsing
# -------------------------------
def parse_news(value: str) -> Optional[Dict[str, Any]]:
    """Parse incoming Kafka message as news JSON."""
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError as exc:
        logger.warning(f"Failed to parse incoming news: {exc}")
    return None


# -------------------------------
# Placeholder for future logic
# (vector DB lookup, semantic matching, etc.)
# -------------------------------
def placeholder_market_matching(news_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Placeholder for advanced matching logic.
    TODO:
        - Embed news text
        - Query vector DB with cosine similarity
        - Match to prediction market entries
        - Add scores / metadata
    """
    news_item["matched_market"] = None
    return news_item


# -------------------------------
# Main Flink Pipeline
# -------------------------------
def main():
    logger.info("Starting News Processing Pipeline")

    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "flink-news-consumer",
        "auto.offset.reset": "earliest",
    }

    # Create Kafka Source
    try:
        kafka_source = FlinkKafkaConsumer(
            topics=NEWS_SOURCE_TOPIC,
            properties=kafka_props,
            deserialization_schema=SimpleStringSchema(),
        )
        logger.info("✓ Kafka source created")
    except Exception as exc:
        logger.error(f"✗ Failed to create Kafka source: {exc}")
        raise

    stream = env.add_source(kafka_source)
    logger.info("✓ Source added to environment")

    # Parse News
    parsed_news = (
        stream.map(parse_news)
              .filter(lambda item: item is not None)
    )
    logger.info("✓ Parsing configured")

    # Filter Only Reuters
    reuters_only = parsed_news.filter(
        lambda item: item.get("source", "").lower() == "reuters"
    )
    logger.info("✓ Reuters filter configured")

    # Placeholder for future NLP + matching logic
    processed = reuters_only.map(
        placeholder_market_matching,
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )
    logger.info("✓ Placeholder matching logic configured")

    # Convert back to JSON string
    processed_strings = processed.map(
        lambda item: json.dumps(item),
        output_type=Types.STRING()
    )

    # Kafka Sink
    try:
        kafka_sink = FlinkKafkaProducer(
            topic=PROCESSED_NEWS_TOPIC,
            producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            serialization_schema=SimpleStringSchema(),
        )
        logger.info("✓ Kafka sink created")
    except Exception as exc:
        logger.error(f"✗ Failed to create Kafka sink: {exc}")
        raise

    processed_strings.add_sink(kafka_sink)
    logger.info("✓ Sink added to pipeline")

    logger.info("Executing job...")
    env.execute("News Processing Job")


if __name__ == "__main__":
    main()
