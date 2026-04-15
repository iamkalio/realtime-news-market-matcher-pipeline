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
  