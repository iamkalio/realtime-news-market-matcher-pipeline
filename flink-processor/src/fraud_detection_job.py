import json
import os
import logging
from typing import Any, Dict, Optional
import requests
import psycopg2
from psycopg2 import pool

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

# Database configuration
PG_HOST = os.getenv("PG_HOST", "pgvector")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "news_match")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

logger.info("Configuration:")
logger.info(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
logger.info(f"  Source Topic: {NEWS_SOURCE_TOPIC}")
logger.info(f"  Sink Topic: {PROCESSED_NEWS_TOPIC}")
logger.info(f"  Embedding Service: {EMBEDDING_SERVICE_URL}")
logger.info(f"  Database: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")


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
    Generate embedding and prepare for database storage.
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
    
    # Store embedding in news_item for database insertion
    news_item["embedding"] = embedding
    news_item["embedding_dim"] = len(embedding)
    news_item["embedding_status"] = "success"
    news_item["matched_market"] = None  # Will be replaced with actual DB query later
    
    return news_item


# -------------------------------
# Database Storage Function with Connection Pooling
# -------------------------------
# Module-level connection pool (created on first use, reused for subsequent calls)
_db_pool = None

def _get_db_pool():
    """Get or create connection pool (lazy initialization)."""
    global _db_pool
    if _db_pool is None:
        try:
            _db_pool = pool.ThreadedConnectionPool(
                minconn=1,      # Minimum connections in pool
                maxconn=10,     # Maximum connections (adjust based on Flink parallelism)
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DATABASE,
                user=PG_USER,
                password=PG_PASSWORD
            )
            logger.info("✓ Postgres connection pool created (min=1, max=10)")
        except Exception as e:
            logger.error(f"✗ Failed to create connection pool: {e}")
            raise
    return _db_pool

def store_news_in_db(news_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Store news and embeddings in PostgreSQL/pgvector using connection pool.
    
    Assumes news_item contains:
        - source
        - date (or published_date)
        - time (or published_time)
        - headline (or news)
        - embedding (list of floats)
    """
    db_pool = _get_db_pool()
    conn = None
    try:
        # Get connection from pool
        conn = db_pool.getconn()
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Extract fields with fallbacks
        source = news_item.get("source")
        published_date = news_item.get("published_date") or news_item.get("date")
        published_time = news_item.get("published_time") or news_item.get("time")
        headline = news_item.get("headline") or news_item.get("news", "")
        embedding = news_item.get("embedding")
        
        # Validate required fields
        if not all([source, published_date, published_time, headline]):
            logger.warning(f"Missing required fields in news_item: {news_item}")
            news_item["db_status"] = "failed_missing_fields"
            db_pool.putconn(conn)
            return news_item
        
        if not embedding:
            logger.warning(f"No embedding found in news_item")
            news_item["db_status"] = "failed_no_embedding"
            db_pool.putconn(conn)
            return news_item
        
        # 1. Insert the main news into news
        insert_news_sql = """
        INSERT INTO news (source, published_date, published_time, headline)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        """
        
        cursor.execute(
            insert_news_sql,
            (source, published_date, published_time, headline)
        )
        
        news_id = cursor.fetchone()[0]
        
        # 2. Insert embedding into news_embeddings
        # Convert list to string format for pgvector: '[0.1,0.2,0.3]'
        embedding_str = '[' + ','.join(map(str, embedding)) + ']'
        
        insert_embedding_sql = """
        INSERT INTO news_embeddings (news_id, embedding)
        VALUES (%s, %s::vector);
        """
        
        cursor.execute(
            insert_embedding_sql,
            (news_id, embedding_str)
        )
        
        logger.info(f"✓ Saved news + embedding into DB (id={news_id}, dim={len(embedding)})")
        
        # Enrich news_item with database info
        news_item["news_id"] = news_id
        news_item["db_status"] = "saved"
        
        # Return connection to pool
        db_pool.putconn(conn)
        conn = None  # Prevent closing in finally
        
        return news_item
        
    except Exception as e:
        logger.error(f"✗ Error saving to database: {e}")
        news_item["db_status"] = f"failed: {str(e)}"
        # Return connection to pool (close if bad)
        if conn:
            try:
                db_pool.putconn(conn, close=True)
            except:
                pass
        return news_item
    finally:
        # Ensure connection is returned to pool
        if conn:
            try:
                db_pool.putconn(conn)
            except:
                pass


# -------------------------------
# JSON Serialization (Remove Embedding)
# -------------------------------

def remove_embedding_and_serialize(item) -> str:
    """
    Remove embedding field and convert to JSON string.
    Handles both Python dict and Flink Map types.
    """
    # Convert to dict if it's a Flink Map type (not a regular Python dict)
    if not isinstance(item, dict):
        # Convert Flink Map to Python dict
        item = dict(item)
    
    # Create a copy without the embedding field
    filtered_item = {k: v for k, v in item.items() if k != "embedding"}
    return json.dumps(filtered_item)


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
    logger.info("✓ Parsing configured - processing all news sources")

    # Step A: Generate embeddings for all news
    embedded = parsed_news.map(
        semantic_market_matching,
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )
    logger.info("✓ Semantic matching (embedding generation) configured")

    # Step B: Store in database
    stored = embedded.map(
        store_news_in_db,
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )
    logger.info("✓ Database storage configured")

    # Convert back to JSON string (remove embedding to reduce message size)
    processed_strings = stored.map(
        remove_embedding_and_serialize,
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
