CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE markets (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    published_date DATE NOT NULL,
    published_time TIME NOT NULL,
    headline TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE market_embeddings (
    id SERIAL PRIMARY KEY,
    market_id INT NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    embedding VECTOR(1536),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Use cosine similarity index
CREATE INDEX IF NOT EXISTS idx_market_embeddings_embedding
ON market_embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 200);
