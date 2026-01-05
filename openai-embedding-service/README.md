# OpenAI Embedding Service

A FastAPI-based microservice for generating text embeddings using OpenAI's API.

## Features

- **Single text embedding**: `POST /embed`
- **Batch embeddings**: `POST /embed/batch`
- **Health check**: `GET /health`
- Uses OpenAI's `text-embedding-3-small` model (1536 dimensions)
- Fast API calls to OpenAI

## Models Available

- `text-embedding-3-small` (1536 dimensions) - Default, cost-effective
- `text-embedding-3-large` (3072 dimensions) - Higher quality
- `text-embedding-ada-002` (1536 dimensions) - Legacy model

## API Endpoints

### Health Check
```bash
curl http://localhost:8002/
```

### Single Embedding
```bash
curl -X POST http://localhost:8002/embed \
  -H "Content-Type: application/json" \
  -d '{"text": "Fed raises interest rates again"}'
```

Response:
```json
{
  "embedding": [0.0132, -0.0221, ...],
  "dimension": 1536,
  "model": "text-embedding-3-small"
}
```

### Batch Embeddings
```bash
curl -X POST http://localhost:8002/embed/batch \
  -H "Content-Type: application/json" \
  -d '{
    "texts": [
      "First news article",
      "Second news article",
      "Third news article"
    ]
  }'
```

Response:
```json
{
  "embeddings": [[...], [...], [...]],
  "dimension": 1536,
  "count": 3,
  "model": "text-embedding-3-small"
}
```

## Environment Variables

- `OPENAI_API_KEY` (required): Your OpenAI API key
- `OPENAI_EMBEDDING_MODEL` (optional): Model to use (default: `text-embedding-3-small`)

## Running with Docker Compose

The service is configured in docker-compose.yml and will start automatically.

## Cost Information

OpenAI `text-embedding-3-small`:
- Cost: $0.02 per 1M tokens
- Very affordable for most use cases
- Example: 1,000 news articles (~500 words each) ≈ $0.50

## Comparison with Local Service

| Feature | Local (SentenceTransformers) | OpenAI |
|---------|------------------------------|--------|
| Cost | Free | ~$0.02/1M tokens |
| Speed | Fast (local) | Network latency |
| Dimensions | 384 | 1536 (small) or 3072 (large) |
| Quality | Good | Excellent |
| Setup | Requires model download | API key only |
| Offline | ✅ Works | ❌ Needs internet |

## Changing the Model

Set environment variable in docker-compose.yml:
```yaml
environment:
  OPENAI_EMBEDDING_MODEL: text-embedding-3-large
```


















