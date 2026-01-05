import os
import time
import logging
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

POLYMARKET_BASE_URL = os.getenv("POLYMARKET_BASE_URL", "https://clob.polymarket.com")
EMBEDDING_SERVICE_URL = os.getenv("EMBEDDING_SERVICE_URL", "http://embedding-service:8001/embed")
PG_HOST = os.getenv("PG_HOST", "pgvector")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "news_match")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
FETCH_LIMIT = int(os.getenv("POLYMARKET_FETCH_LIMIT", "20"))


def get_db_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )


def fetch_trending_markets() -> List[Dict]:
    url = f"{POLYMARKET_BASE_URL}/markets"
    params = {"limit": FETCH_LIMIT, "active": "true", "closed": "false"}
    
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        markets = r.json()
        
        if isinstance(markets, dict):
            markets = markets.get("data", markets.get("markets", []))
        
        if isinstance(markets, list):
            markets = markets[:FETCH_LIMIT]
        
        logger.info(f"Fetched {len(markets)} markets")
        return markets if isinstance(markets, list) else []
    except Exception as e:
        logger.error(f"Error fetching markets: {e}")
        return []


def generate_embedding(text: str) -> Optional[List[float]]:
    try:
        r = requests.post(EMBEDDING_SERVICE_URL, json={"text": text}, timeout=10)
        r.raise_for_status()
        embedding = r.json().get("embedding")
        return embedding if embedding else None
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        return None


def upsert_market(cursor, market: Dict) -> Optional[int]:
    slug = market.get("market_slug") or market.get("slug")
    question = market.get("question")
    is_resolved = market.get("closed", False) or market.get("resolved", False) or market.get("is_resolved", False)
    
    if not slug or not question:
        return None
    
    sql = """
    INSERT INTO markets (source, slug, question, is_resolved)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (slug) DO UPDATE SET
        question = EXCLUDED.question,
        is_resolved = EXCLUDED.is_resolved,
        updated_at = NOW()
    RETURNING id;
    """
    
    try:
        cursor.execute(sql, ("polymarket", slug, question, is_resolved))
        result = cursor.fetchone()
        if result:
            if isinstance(result, dict) or hasattr(result, 'get'):
                return result.get("id")
            else:
                return result[0] if result else None
        return None
    except Exception as e:
        logger.error(f"Error upserting market {slug}: {e}", exc_info=True)
        return None


def upsert_market_embedding(cursor, market_id: int, embedding: List[float]) -> bool:
    if not embedding:
        return False
    
    embedding_str = '[' + ','.join(map(str, embedding)) + ']'
    sql = """
    INSERT INTO market_embeddings (market_id, embedding)
    VALUES (%s, %s::vector)
    ON CONFLICT (market_id) DO UPDATE SET
        embedding = EXCLUDED.embedding,
        created_at = NOW();
    """
    
    try:
        cursor.execute(sql, (market_id, embedding_str))
        return True
    except Exception as e:
        logger.error(f"Error upserting embedding: {e}")
        return False


def run_ingestion():
    markets = fetch_trending_markets()
    if not markets:
        return
    
    conn = get_db_connection()
    conn.autocommit = True
    success_count = 0
    error_count = 0
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Check if markets table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'markets'
                ) as exists;
            """)
            result = cursor.fetchone()
            table_exists = result.get("exists") if hasattr(result, "get") else result[0]
            
            if not table_exists:
                logger.warning("Markets table does not exist yet, waiting for migration...")
                return
            
            for market in markets:
                try:
                    slug = market.get("market_slug") or market.get("slug")
                    question = market.get("question")
                    
                    if not question or not slug:
                        error_count += 1
                        continue
                    
                    embedding = generate_embedding(question)
                    if not embedding:
                        error_count += 1
                        continue
                    
                    market_id = upsert_market(cursor, market)
                    if not market_id:
                        logger.warning(f"Failed to get market_id for {slug}")
                        error_count += 1
                        continue
                    
                    if upsert_market_embedding(cursor, market_id, embedding):
                        logger.info(f"Saved market: {slug} (id={market_id})")
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    logger.error(f"Error processing market {market.get('market_slug', 'unknown')}: {e}", exc_info=True)
                    error_count += 1
    except Exception as e:
        import traceback
        logger.error(f"Error during ingestion: {e}\n{traceback.format_exc()}")
        error_count += 1
    finally:
        conn.close()
    
    logger.info(f"Ingestion complete: {success_count} successful, {error_count} errors")


def main():
    interval_seconds = int(os.getenv("POLYMARKET_INTERVAL_SECONDS", "120"))
    
    while True:
        try:
            run_ingestion()
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()

