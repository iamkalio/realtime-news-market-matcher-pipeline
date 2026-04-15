import json
import os
import logging
import threading
from typing import Any, Dict, Optional, List
import requests
import psycopg2
from psycopg2 import pool, OperationalError

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer,
    FlinkKafkaProducer,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
NEWS_SOURCE_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "news_stream")
PROCESSED_NEWS_TOPIC = os.getenv("KAFKA_SINK_TOPIC", "processed_news")

EMBEDDING_SERVICE_URL = os.getenv("EMBEDDING_SERVICE_URL", "http://embedding-service:8001/embed")
EMBEDDING_REQUEST_TIMEOUT = 2.0

PG_HOST = os.getenv("PG_HOST", "pgvector")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "news_match")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

MIN_SIMILARITY_THRESHOLD = float(os.getenv("MIN_SIMILARITY_THRESHOLD", "0.5"))

# Database connection pool configuration
DB_POOL_MIN_CONN = 1
DB_POOL_MAX_CONN = 10

logger.info("Configuration:")
logger.info(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
logger.info(f"  Source Topic: {NEWS_SOURCE_TOPIC}")
logger.info(f"  Sink Topic: {PROCESSED_NEWS_TOPIC}")
logger.info(f"  Embedding Service: {EMBEDDING_SERVICE_URL}")
logger.info(f"  Database: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
logger.info(f"  Min Similarity Threshold: {MIN_SIMILARITY_THRESHOLD}")


# Helper Functions
def _embedding_to_vector_string(embedding: List[float]) -> str:
    """Convert embedding list to pgvector string format: '[val1,val2,val3]'."""
    return '[' + ','.join(map(str, embedding)) + ']'


def _normalize_dict(item: Any) -> Dict[str, Any]:
    """Convert Flink Map types to Python dict if needed."""
    if not isinstance(item, dict):
        return dict(item)
    return item


# JSON Parsing
def parse_news(value: str) -> Optional[Dict[str, Any]]:
    """Parse incoming Kafka message as news JSON."""
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError as exc:
        logger.warning(f"Failed to parse incoming news: {exc}")
    return None


# Embedding Generation
def generate_embedding(text: str) -> Optional[List[float]]:
    """Call the embedding microservice to generate embeddings."""
    if not text:
        return None
        
    try:
        response = requests.post(
            EMBEDDING_SERVICE_URL,
            json={"text": text},
            timeout=EMBEDDING_REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            result = response.json()
            embedding = result.get("embedding")
            if embedding and isinstance(embedding, list):
                return embedding
        logger.warning(f"Embedding service returned status {response.status_code}")
        return None
    except requests.exceptions.Timeout:
        logger.error(f"Embedding service timeout after {EMBEDDING_REQUEST_TIMEOUT}s")
        return None
    except Exception as exc:
        logger.error(f"Embedding error: {exc}")
        return None


# Semantic Market Matching
def semantic_market_matching(news_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate embedding for news headline and match to markets.
    Adds embedding and match information to news_item.
    """
    headline = news_item.get("headline") or news_item.get("news", "")
    
    if not headline:
        logger.warning("No text found in news item")
        news_item["matched_market"] = None
        news_item["embedding_status"] = "no_text"
        return news_item
    
    embedding = generate_embedding(headline)
    if embedding is None:
        logger.warning(f"Failed to generate embedding for: {headline[:50]}...")
        news_item["matched_market"] = None
        news_item["embedding_status"] = "failed"
        return news_item
    
    news_item["embedding"] = embedding
    news_item["embedding_dim"] = len(embedding)
    news_item["embedding_status"] = "success"
    
    # match_market already applies threshold filtering
    match = match_market(embedding)
    if match:
        news_item["matched_market"] = match["slug"]
        news_item["matched_market_id"] = match["market_id"]
        news_item["matched_market_question"] = match["question"]
        news_item["match_score"] = match["similarity"]
    else:
        news_item["matched_market"] = None
        news_item["match_score"] = None
    
    return news_item


# Database Connection Pooling
_db_pool: Optional[pool.ThreadedConnectionPool] = None
_db_pool_lock = threading.Lock()


def _get_db_pool() -> pool.ThreadedConnectionPool:
    """Get or create connection pool (thread-safe lazy initialization)."""
    global _db_pool
    if _db_pool is None:
        with _db_pool_lock:
            # Double-check pattern to avoid race condition
            if _db_pool is None:
                try:
                    _db_pool = pool.ThreadedConnectionPool(
                        minconn=DB_POOL_MIN_CONN,
                        maxconn=DB_POOL_MAX_CONN,
                        host=PG_HOST,
                        port=PG_PORT,
                        database=PG_DATABASE,
                        user=PG_USER,
                        password=PG_PASSWORD
                    )
                    logger.info("Postgres connection pool created")
                except Exception as exc:
                    logger.error(f"Failed to create connection pool: {exc}")
                    raise
    return _db_pool


def match_market(embedding: List[float]) -> Optional[Dict[str, Any]]:
    """
    Find best matching market for a news embedding using cosine similarity.
    Returns match only if similarity >= MIN_SIMILARITY_THRESHOLD.
    """
    if not embedding:
        return None
        
    db_pool = _get_db_pool()
    conn = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # Check if both tables exist
        cursor.execute("""
            SELECT 
                EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'markets') as markets_exists,
                EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'market_embeddings') as embeddings_exists;
        """)
        markets_exists, embeddings_exists = cursor.fetchone()
        
        if not markets_exists or not embeddings_exists:
            logger.debug("Markets tables do not exist yet, skipping match")
            return None
        
        # Check if there are any markets with embeddings
        cursor.execute("SELECT COUNT(*) FROM market_embeddings;")
        if cursor.fetchone()[0] == 0:
            logger.debug("No market embeddings available yet, skipping match")
            return None
        
        embedding_str = _embedding_to_vector_string(embedding)
        
        cursor.execute("""
            SELECT
                m.id,
                m.slug,
                m.question,
                1 - (me.embedding <=> %s::vector) AS similarity
            FROM market_embeddings me
            JOIN markets m ON m.id = me.market_id
            WHERE m.is_resolved = false
            ORDER BY me.embedding <=> %s::vector
            LIMIT 1;
        """, (embedding_str, embedding_str))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        market_id, slug, question, similarity = row
        similarity = float(similarity)
        
        if similarity < MIN_SIMILARITY_THRESHOLD:
            logger.debug(
                f"Match similarity {similarity:.3f} below threshold {MIN_SIMILARITY_THRESHOLD}, "
                f"rejecting match to '{slug}'"
            )
            return None
        
        logger.info(
            f"Match found: '{slug}' (similarity: {similarity:.3f}, "
            f"threshold: {MIN_SIMILARITY_THRESHOLD})"
        )
        return {
            "market_id": market_id,
            "slug": slug,
            "question": question,
            "similarity": similarity
        }
    except OperationalError as exc:
        logger.warning(f"Database connection error during market matching: {exc}")
        return None
    except Exception as exc:
        logger.error(f"Error matching market: {exc}")
        return None
    finally:
        if conn:
            db_pool.putconn(conn)

def store_news_in_db(news_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Store news and embeddings in PostgreSQL/pgvector using connection pool.
    
    Requires news_item fields:
        - source, published_date/date, published_time/time, headline/news, embedding
    """
    db_pool = _get_db_pool()
    conn = None
    try:
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
            logger.warning("Missing required fields in news_item")
            news_item["db_status"] = "failed_missing_fields"
            return news_item
        
        if not embedding:
            logger.warning("No embedding found in news_item")
            news_item["db_status"] = "failed_no_embedding"
            return news_item
        
        # Insert news record
        cursor.execute(
            """
            INSERT INTO news (source, published_date, published_time, headline)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            (source, published_date, published_time, headline)
        )
        news_id = cursor.fetchone()[0]
        
        # Insert embedding
        embedding_str = _embedding_to_vector_string(embedding)
        cursor.execute(
            """
            INSERT INTO news_embeddings (news_id, embedding)
            VALUES (%s, %s::vector);
            """,
            (news_id, embedding_str)
        )
        
        logger.info(f"Saved news + embedding to DB (id={news_id}, dim={len(embedding)})")
        news_item["news_id"] = news_id
        news_item["db_status"] = "saved"
        return news_item
        
    except Exception as exc:
        logger.error(f"Error saving to database: {exc}")
        news_item["db_status"] = f"failed: {str(exc)}"
        return news_item
    finally:
        if conn:
            try:
                db_pool.putconn(conn)
            except Exception:
                pass


# Filtering and Serialization
def has_matched_market(item: Any) -> bool:
    """Check if item has a matched market (handles both dict and Flink Map types)."""
    if item is None:
        return False
    item_dict = _normalize_dict(item)
    return item_dict.get("matched_market") is not None


def remove_embedding_and_serialize(item: Any) -> str:
    """Remove embedding field and convert to JSON string (to reduce message size)."""
    item_dict = _normalize_dict(item)
    filtered_item = {k: v for k, v in item_dict.items() if k != "embedding"}
    return json.dumps(filtered_item)


# Main Flink Pipeline
def main():
    """Build and execute the Flink news processing pipeline."""
    logger.info("Starting News Processing Pipeline")

    env = StreamExecutionEnvironment.get_execution_environment()

    # Configure Kafka source
    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "flink-news-consumer",
        "auto.offset.reset": "earliest",
    }

    try:
        kafka_source = FlinkKafkaConsumer(
            topics=NEWS_SOURCE_TOPIC,
            properties=kafka_props,
            deserialization_schema=SimpleStringSchema(),
        )
        logger.info("Kafka source created")
    except Exception as exc:
        logger.error(f"Failed to create Kafka source: {exc}")
        raise

    # Build pipeline
    stream = env.add_source(kafka_source)
    
    parsed_news = stream.map(parse_news).filter(lambda item: item is not None)
    
    # Generate embeddings and match to markets
    embedded = parsed_news.map(
        semantic_market_matching,
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )
    
    # Store all news in database
    stored = embedded.map(
        store_news_in_db,
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )
    
    # Filter to only matched news
    matched_only = stored.filter(has_matched_market)
    
    # Serialize (removing embedding to reduce message size)
    processed_strings = matched_only.map(
        remove_embedding_and_serialize,
        output_type=Types.STRING()
    )

    # Configure Kafka sink
    try:
        kafka_sink = FlinkKafkaProducer(
            topic=PROCESSED_NEWS_TOPIC,
            producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            serialization_schema=SimpleStringSchema(),
        )
        logger.info("Kafka sink created")
    except Exception as exc:
        logger.error(f"Failed to create Kafka sink: {exc}")
        raise

    processed_strings.add_sink(kafka_sink)
    logger.info("Pipeline configured, executing job...")
    env.execute("News Processing Job")


if __name__ == "__main__":
    main()
