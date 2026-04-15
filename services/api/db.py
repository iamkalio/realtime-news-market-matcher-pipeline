import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Optional

PG_HOST = os.getenv("PG_HOST", "pgvector")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "news_match")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")


def get_db_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )


def get_markets(limit: int = 100, is_resolved: Optional[bool] = None) -> List[Dict]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = "SELECT id, source, slug, question, is_resolved, created_at, updated_at FROM markets"
            params = []
            
            if is_resolved is not None:
                query += " WHERE is_resolved = %s"
                params.append(is_resolved)
            
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            # Convert datetime objects to strings for JSON serialization
            result = []
            for row in rows:
                row_dict = dict(row)
                # Convert datetime to ISO format string
                if row_dict.get('created_at'):
                    row_dict['created_at'] = row_dict['created_at'].isoformat() if hasattr(row_dict['created_at'], 'isoformat') else str(row_dict['created_at'])
                if row_dict.get('updated_at'):
                    row_dict['updated_at'] = row_dict['updated_at'].isoformat() if hasattr(row_dict['updated_at'], 'isoformat') else str(row_dict['updated_at'])
                result.append(row_dict)
            return result
    except Exception as e:
        print(f"Error fetching markets: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_market_stats() -> Dict:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_markets,
                    COUNT(*) FILTER (WHERE is_resolved = false) as active_markets,
                    COUNT(*) FILTER (WHERE is_resolved = true) as resolved_markets,
                    COUNT(DISTINCT me.market_id) as markets_with_embeddings
                FROM markets m
                LEFT JOIN market_embeddings me ON m.id = me.market_id
            """)
            result = cursor.fetchone()
            return dict(result) if result else {
                "total_markets": 0,
                "active_markets": 0,
                "resolved_markets": 0,
                "markets_with_embeddings": 0
            }
    except Exception as e:
        print(f"Error fetching market stats: {e}")
        return {
            "total_markets": 0,
            "active_markets": 0,
            "resolved_markets": 0,
            "markets_with_embeddings": 0
        }
    finally:
        if conn:
            conn.close()

