# Polymarket Ingestion Service

This service fetches trending markets from Polymarket, generates embeddings, and stores them in PostgreSQL with pgvector.

## Overview

The Polymarket ingestion pipeline:
- Fetches trending markets from Polymarket API
- Normalizes them into the `markets` table
- Generates embeddings using the existing embedding service
- Stores embeddings in `market_embeddings` (pgvector)
- Runs every 2 minutes (configurable)
- Is idempotent (no duplicates via `ON CONFLICT`)

## Architecture

```
[ Polymarket API ]
        ↓
[ Market Ingestion Service ]
        ↓
[ Embedding Service ]
        ↓
[ Postgres + pgvector ]
```

## Database Schema

### `markets` table
- `id`: Primary key
- `source`: Always "polymarket"
- `slug`: Unique market identifier
- `question`: Market question text
- `is_resolved`: Whether the market is resolved
- `created_at`: Timestamp
- `updated_at`: Timestamp

### `market_embeddings` table
- `id`: Primary key
- `market_id`: Foreign key to `markets.id`
- `embedding`: Vector(384) - same dimension as news embeddings
- `created_at`: Timestamp

## Configuration

Environment variables:
- `PG_HOST`: Database host (default: `pgvector`)
- `PG_PORT`: Database port (default: `5432`)
- `PG_DATABASE`: Database name (default: `news_match`)
- `PG_USER`: Database user (default: `postgres`)
- `PG_PASSWORD`: Database password (default: `postgres`)
- `EMBEDDING_SERVICE_URL`: Embedding service endpoint (default: `http://embedding-service:8001/embed`)
- `POLYMARKET_BASE_URL`: Polymarket API base URL (default: `https://clob.polymarket.com`)
- `POLYMARKET_FETCH_LIMIT`: Number of markets to fetch (default: `20`)
- `POLYMARKET_INTERVAL_SECONDS`: Ingestion interval in seconds (default: `120` = 2 minutes)

## Usage

### Running with Docker Compose

The service is automatically started with `docker-compose up`:

```bash
docker-compose up polymarket-ingest
```

### Running locally

```bash
pip install -r requirements.txt
python ingest.py
```

## Features

- **Idempotent**: Uses `ON CONFLICT` to prevent duplicates
- **Error handling**: Continues processing even if individual markets fail
- **Logging**: Comprehensive logging for debugging
- **Configurable**: All settings via environment variables
- **Resilient**: Handles API failures gracefully

## Future Enhancements

- Exclude resolved markets
- Add market lifecycle states
- Re-embed markets when descriptions change
- Add alerts/countdowns for market resolution

