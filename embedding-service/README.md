# Embedding Microservice

A FastAPI-based microservice for generating text embeddings using SentenceTransformers.

## Features

- **Single text embedding**: `POST /embed`
- **Batch embeddings**: `POST /embed/batch` (up to 100 texts)
- **Health check**: `GET /health`
- Uses `all-MiniLM-L6-v2` model (384 dimensions)
- Fast and efficient batch processing

## API Endpoints

### Health Check
```bash
curl http://localhost:8001/
```

### Single Embedding
```bash
curl -X POST http://localhost:8001/embed \
  -H "Content-Type: application/json" \
  -d '{"text": "Fed raises interest rates again"}'
```

Response:
```json
{
  "embedding": [0.0132, -0.0221, ...],
  "dimension": 384
}
```

### Batch Embeddings
```bash
curl -X POST http://localhost:8001/embed/batch \
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
  "dimension": 384,
  "count": 3
}
```

## Running Locally

### With Docker Compose
```bash
docker compose up embedding-service
```

### Standalone
```bash
cd embedding-service
pip install -r requirements.txt
python service.py
```

## Model Information

- **Model**: `all-MiniLM-L6-v2`
- **Dimensions**: 384
- **Max Sequence Length**: 256 tokens
- **Speed**: ~3000 sentences/second on CPU

## Changing the Model

To use a different model, edit `service.py`:

```python
# Options:
model = SentenceTransformer("all-MiniLM-L6-v2")  # Fast, 384 dim
model = SentenceTransformer("all-mpnet-base-v2")  # Better quality, 768 dim
model = SentenceTransformer("nomic-ai/nomic-embed-text-v1")  # Nomic
```

## Integration Example

```python
import requests

# Single embedding
response = requests.post(
    "http://localhost:8001/embed",
    json={"text": "Your text here"}
)
embedding = response.json()["embedding"]

# Batch embeddings
response = requests.post(
    "http://localhost:8001/embed/batch",
    json={"texts": ["Text 1", "Text 2", "Text 3"]}
)
embeddings = response.json()["embeddings"]
```

## Performance

- Single request: ~10-50ms
- Batch of 10: ~50-100ms
- Batch of 100: ~200-500ms

Batch processing is significantly more efficient than individual requests.

