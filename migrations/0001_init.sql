CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    published_date DATE NOT NULL,
    published_time TIME NOT NULL,
    headline TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news_embeddings (
    id SERIAL PRIMARY KEY,
    news_id INT NOT NULL REFERENCES news(id) ON DELETE CASCADE,
    embedding VECTOR(384),  -- Changed from 1536 to 384 for local embedding service (all-MiniLM-L6-v2)
    created_at TIMESTAMP DEFAULT NOW()
);

-- Use cosine similarity index
CREATE INDEX IF NOT EXISTS idx_news_embeddings_embedding
ON news_embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 200);
