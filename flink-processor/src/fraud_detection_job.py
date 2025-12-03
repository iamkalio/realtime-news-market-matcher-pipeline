import json
import os
import logging
from typing import Any, Dict, Optional
import requests

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

# Embedding service configuration
EMBEDDING_SERVICE_URL = os.getenv("EMBEDDING_SERVICE_URL", "http://embedding-service:8001/embed")

logger.info("Configuration:")
logger.info(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
logger.info(f"  Source Topic: {NEWS_SOURCE_TOPIC}")
logger.info(f"  Sink Topic: {PROCESSED_NEWS_TOPIC}")
logger.info(f"  Embedding Service: {EMBEDDING_SERVICE_URL}")


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
# Embedding Generation
# -------------------------------
def generate_embedding(text: str) -> Optional[list]:
    """
    Call the embedding microservice to generate embeddings.
    
    Args:
        text: Text to embed
        
    Returns:
        List of floats (embedding vector) or None if failed
    """
    try:
        response = requests.post(
            EMBEDDING_SERVICE_URL,
            json={"text": text},
            timeout=2.0  # 2 second timeout
        )
        if response.status_code == 200:
            result = response.json()
            return result.get("embedding")
        else:
            logger.warning(f"Embedding service returned status {response.status_code}")
            return None
    except requests.exceptions.Timeout:
        logger.error("Embedding service timeout")
        return None
    except Exception as e:
        logger.error(f"Embedding error: {e}")
        return None


# -------------------------------
# Semantic Market Matching
# -------------------------------
def semantic_market_matching(news_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate embedding and match to prediction markets.
    
    TODO:
        - Query pgvector with cosine similarity
        - Match to prediction market entries
        - Add scores / metadata
    """
    # Get the news text (could be from different fields)
    headline = news_item.get("headline") or news_item.get("news", "")
    
    if not headline:
        logger.warning("No text found in news item")
        news_item["matched_market"] = None
        news_item["embedding_status"] = "no_text"
        return news_item
    
    # Generate embedding
    embedding = generate_embedding(headline)
    
    if embedding is None:
        logger.warning(f"Failed to generate embedding for: {headline[:50]}")
        news_item["matched_market"] = None
        news_item["embedding_status"] = "failed"
        return news_item
    
    # Successfully generated embedding
    logger.info(f"Generated embedding (dim={len(embedding)}) for: {headline[:50]}...")
    
    # TODO: Query pgvector here
    # Example:
    # matched = query_vector_db(embedding)
    # news_item["matched_market"] = matched["market_id"]
    # news_item["match_score"] = matched["similarity"]
    
    # For now, just indicate we have the embedding
    news_item["embedding_dim"] = len(embedding)
    news_item["embedding_status"] = "success"
    news_item["matched_market"] = None  # Will be replaced with actual DB query
    
    # Note: We don't store the full embedding in Kafka (too large)
    # Instead, it would be stored in pgvector
    
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

    # Semantic matching with embeddings
    processed = reuters_only.map(
        semantic_market_matching,
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )
    logger.info("✓ Semantic matching logic configured")

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
