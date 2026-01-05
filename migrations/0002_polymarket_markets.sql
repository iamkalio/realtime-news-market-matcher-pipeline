-- Migration: Create markets table for Polymarket data
CREATE TABLE IF NOT EXISTS markets (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'polymarket',
    slug TEXT UNIQUE NOT NULL,
    question TEXT NOT NULL,
    is_resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_embeddings (
    id SERIAL PRIMARY KEY,
    market_id INT NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    embedding VECTOR(384),  -- Same dimension as news embeddings (all-MiniLM-L6-v2)
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(market_id)
);

-- Use cosine similarity index for market embeddings
CREATE INDEX IF NOT EXISTS idx_market_embeddings_embedding
ON market_embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 200);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_markets_slug ON markets(slug);
CREATE INDEX IF NOT EXISTS idx_markets_is_resolved ON markets(is_resolved);

