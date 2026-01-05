# Poly-Forcaster System Design

## Overview

Poly-Forcaster is a real-time news-to-market matching system that uses semantic similarity to match news headlines with Polymarket prediction markets. The system processes streaming news, generates embeddings, and matches them against market questions using cosine similarity.

## Architecture Diagram

```mermaid
graph TB
    subgraph "Data Sources"
        PM_API[Polymarket API]
        KAFKA_PROD[Kafka Producer<br/>Fake News Generator]
    end

    subgraph "Ingestion Layer"
        POLY_INGEST[Polymarket Ingestion Service<br/>Runs every 2 minutes]
        EMBED_SVC[Embedding Service<br/>SentenceTransformers]
    end

    subgraph "Message Queue"
        KAFKA[Kafka Broker]
        NEWS_STREAM[news_stream Topic]
        PROC_NEWS[processed_news Topic]
    end

    subgraph "Stream Processing"
        FLINK_JM[Flink Job Manager]
        FLINK_TM[Flink Task Manager<br/>News Processing Pipeline]
        JOB_SUBMIT[Job Submitter]
    end

    subgraph "Storage Layer"
        PG[(PostgreSQL + pgvector)]
        NEWS_TBL[(news table)]
        NEWS_EMB[(news_embeddings)]
        MARKETS_TBL[(markets table)]
        MARKETS_EMB[(market_embeddings)]
    end

    subgraph "API Layer"
        API_SVC[API Service<br/>FastAPI]
        API_NEWS[/api/news]
        API_PROC[/api/processed]
        API_MARKETS[/api/markets]
        API_STATS[/api/stats]
    end

    %% Data Flow
    PM_API -->|Fetch Markets| POLY_INGEST
    POLY_INGEST -->|Generate Embeddings| EMBED_SVC
    EMBED_SVC -->|Return Embeddings| POLY_INGEST
    POLY_INGEST -->|Store Markets & Embeddings| MARKETS_TBL
    POLY_INGEST -->|Store Embeddings| MARKETS_EMB

    KAFKA_PROD -->|Publish News| NEWS_STREAM
    NEWS_STREAM -->|Consume| FLINK_TM
    
    FLINK_TM -->|Generate Embeddings| EMBED_SVC
    EMBED_SVC -->|Return Embeddings| FLINK_TM
    FLINK_TM -->|Store News & Embeddings| NEWS_TBL
    FLINK_TM -->|Store Embeddings| NEWS_EMB
    FLINK_TM -->|Match Against Markets| MARKETS_EMB
    MARKETS_EMB -->|Query Similarity| FLINK_TM
    FLINK_TM -->|Filter by Threshold ≥0.5| PROC_NEWS
    
    PROC_NEWS -->|Consume| API_SVC
    NEWS_STREAM -->|Consume| API_SVC
    MARKETS_TBL -->|Query| API_SVC
    
    API_SVC --> API_NEWS
    API_SVC --> API_PROC
    API_SVC --> API_MARKETS
    API_SVC --> API_STATS

    FLINK_JM -->|Orchestrate| FLINK_TM
    JOB_SUBMIT -->|Submit Job| FLINK_JM

    NEWS_TBL -.->|FK| NEWS_EMB
    MARKETS_TBL -.->|FK| MARKETS_EMB
```

## Component Details

### 1. Polymarket Ingestion Service

**Purpose:** Periodically fetch trending markets from Polymarket API and store them with embeddings.

**Flow:**
1. Fetches markets from Polymarket API (every 2 minutes)
2. For each market:
   - Extracts question text
   - Generates embedding via Embedding Service
   - Stores market in `markets` table
   - Stores embedding in `market_embeddings` table

**Key Features:**
- Idempotent (uses `ON CONFLICT` to prevent duplicates)
- Only processes unresolved markets
- Handles API errors gracefully

### 2. Flink Stream Processing Pipeline

**Purpose:** Process streaming news, generate embeddings, match to markets, and filter by similarity threshold.

**Pipeline Stages:**

```
news_stream → Parse → Generate Embedding → Match Market → Store in DB → Filter (≥0.5) → processed_news
```

**Processing Steps:**

1. **Parse News:** Extract headline/text from Kafka message
2. **Generate Embedding:** Call Embedding Service to create 384-dim vector
3. **Match Market:** 
   - Query `market_embeddings` using cosine similarity
   - Find best match (highest similarity)
   - Apply threshold filter (≥0.5)
4. **Store in Database:**
   - Save news to `news` table
   - Save embedding to `news_embeddings` table
5. **Filter & Publish:**
   - Only send to `processed_news` if match_score ≥ 0.5
   - Enrich with market details (slug, question, score)

### 3. Embedding Service

**Purpose:** Generate semantic embeddings for text using SentenceTransformers.

**Model:** `all-MiniLM-L6-v2` (384 dimensions)

**Endpoints:**
- `POST /embed` - Generate embedding for text
- `GET /health` - Health check

**Usage:**
- Used by Polymarket Ingestion Service (for market questions)
- Used by Flink Processor (for news headlines)

### 4. Database Schema

#### `news` Table
```sql
- id (SERIAL PRIMARY KEY)
- source (TEXT)
- published_date (DATE)
- published_time (TIME)
- headline (TEXT)
- created_at (TIMESTAMP)
```

#### `news_embeddings` Table
```sql
- id (SERIAL PRIMARY KEY)
- news_id (INT, FK to news.id)
- embedding (VECTOR(384))
- created_at (TIMESTAMP)
```

#### `markets` Table
```sql
- id (SERIAL PRIMARY KEY)
- source (TEXT, default 'polymarket')
- slug (TEXT UNIQUE)
- question (TEXT)
- is_resolved (BOOLEAN)
- created_at (TIMESTAMP)
- updated_at (TIMESTAMP)
```

#### `market_embeddings` Table
```sql
- id (SERIAL PRIMARY KEY)
- market_id (INT, FK to markets.id, UNIQUE)
- embedding (VECTOR(384))
- created_at (TIMESTAMP)
```

**Indexes:**
- `idx_news_embeddings_embedding` - IVFFlat index for cosine similarity
- `idx_market_embeddings_embedding` - IVFFlat index for cosine similarity
- `idx_markets_slug` - For fast lookups
- `idx_markets_is_resolved` - For filtering unresolved markets

### 5. Matching Algorithm

**Similarity Metric:** Cosine Similarity

**Query:**
```sql
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
```

**Threshold:** 0.5 (50% similarity minimum)

**Process:**
1. Generate embedding for news headline
2. Find nearest market embedding using cosine distance
3. Calculate similarity: `1 - distance`
4. If similarity ≥ 0.5: accept match
5. If similarity < 0.5: reject match

### 6. API Service

**Endpoints:**

- `GET /api/news` - Get raw news from `news_stream` topic
- `GET /api/processed` - Get matched news from `processed_news` topic
- `GET /api/markets` - Get list of markets from database
- `GET /api/stats` - Get pipeline statistics (news count, matches, market stats)

**Features:**
- Real-time consumption from Kafka topics
- Database queries for markets
- WebSocket support for live updates

## Data Flow

### Market Ingestion Flow

```
Polymarket API
    ↓
Polymarket Ingestion Service
    ↓
Embedding Service (generate embedding)
    ↓
PostgreSQL (markets + market_embeddings)
```

### News Processing Flow

```
Kafka Producer
    ↓
news_stream Topic
    ↓
Flink Task Manager
    ├─→ Embedding Service (generate embedding)
    ├─→ PostgreSQL (news + news_embeddings)
    ├─→ PostgreSQL (query market_embeddings for match)
    └─→ Filter (similarity ≥ 0.5)
    ↓
processed_news Topic
    ↓
API Service
    ↓
/api/processed Endpoint
```

## Technology Stack

- **Stream Processing:** Apache Flink (Python API)
- **Message Queue:** Apache Kafka
- **Database:** PostgreSQL 16 with pgvector extension
- **Embeddings:** SentenceTransformers (all-MiniLM-L6-v2)
- **API:** FastAPI (Python)
- **Orchestration:** Docker Compose

## Key Design Decisions

1. **Separate Ingestion for Markets:** Markets change slowly, so periodic ingestion (every 2 minutes) is sufficient, unlike real-time news streaming.

2. **Threshold Filtering:** Only matches with similarity ≥ 0.5 are sent to `processed_news` to maintain high signal-to-noise ratio.

3. **Embedding Reuse:** Same embedding model (all-MiniLM-L6-v2) used for both news and markets ensures semantic compatibility.

4. **Idempotency:** Market ingestion uses `ON CONFLICT` to prevent duplicates when re-running.

5. **Connection Pooling:** Database connections are pooled for efficiency in high-throughput scenarios.

6. **Resolved Market Filtering:** Only unresolved markets are considered for matching to avoid stale predictions.

## Performance Characteristics

- **Throughput:** Processes news as fast as Kafka can deliver (typically 4-second intervals)
- **Latency:** End-to-end processing time ~1-2 seconds (embedding generation + DB operations)
- **Scalability:** Flink can scale horizontally with multiple task managers
- **Storage:** pgvector IVFFlat indexes enable fast similarity searches even with thousands of markets

## Monitoring & Observability

- **Logs:** All services log match scores, rejections, and errors
- **API Stats:** `/api/stats` endpoint provides real-time pipeline metrics
- **Database:** Direct queries for debugging and analysis









