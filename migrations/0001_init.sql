CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS markets (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    published_date DATE NOT NULL,
    published_time TIME NOT NULL,
    headline TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_embeddings (
    id SERIAL PRIMARY KEY,
    market_id INT NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    embedding VECTOR(384),  -- Changed from 1536 to 384 for local embedding service (all-MiniLM-L6-v2)
    created_at TIMESTAMP DEFAULT NOW()
);

-- Use cosine similarity index
CREATE INDEX IF NOT EXISTS idx_market_embeddings_embedding
ON market_embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 200);
