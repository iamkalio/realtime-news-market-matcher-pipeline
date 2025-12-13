# Embedding Services Comparison

You now have **two embedding services** running in your pipeline:

## Service Overview

| Service | Port | Model | Dimensions | Type |
|---------|------|-------|------------|------|
| **embedding-service** | 8001 | all-MiniLM-L6-v2 | 384 | Local (Free) |
| **openai-embedding-service** | 8002 | text-embedding-3-small | 1536 | API (Paid) |

## 1. Local Embedding Service (Port 8001)

**Technology:** SentenceTransformers (local model)

**Pros:**
- ✅ Free (no API costs)
- ✅ Fast (runs locally)
- ✅ Works offline
- ✅ No rate limits
- ✅ Privacy (data stays local)

**Cons:**
- ❌ Lower quality embeddings
- ❌ Smaller dimensions (384 vs 1536)
- ❌ Requires more RAM/CPU
- ❌ Model download on first run

**Access:**
```bash
# Health check
curl http://localhost:8001/health

# Generate embedding
curl -X POST http://localhost:8001/embed \
  -H "Content-Type: application/json" \
  -d '{"text": "Your text here"}'
```

**Swagger UI:** http://localhost:8001/docs

## 2. OpenAI Embedding Service (Port 8002)

**Technology:** OpenAI API (text-embedding-3-small)

**Pros:**
- ✅ Higher quality embeddings
- ✅ Larger dimensions (1536)
- ✅ Lighter resource usage
- ✅ No model download needed
- ✅ Better semantic understanding

**Cons:**
- ❌ Costs money (~$0.02 per 1M tokens)
- ❌ Requires API key
- ❌ Requires internet connection
- ❌ API rate limits apply
- ❌ Data sent to OpenAI

**Access:**
```bash
# Health check
curl http://localhost:8002/health

# Generate embedding
curl -X POST http://localhost:8002/embed \
  -H "Content-Type: application/json" \
  -d '{"text": "Your text here"}'
```

**Swagger UI:** http://localhost:8002/docs

## Current Flink Configuration

Your Flink job is currently configured to use the **local service**:

```python
EMBEDDING_SERVICE_URL = "http://embedding-service:8001/embed"
```

## Switching Between Services

### Option 1: Use Environment Variable

In `docker-compose.yml`, change the Flink environment variable:

**For Local (Free):**
```yaml
environment:
  EMBEDDING_SERVICE_URL: http://embedding-service:8001/embed
```

**For OpenAI (Paid, Better Quality):**
```yaml
environment:
  EMBEDDING_SERVICE_URL: http://openai-embedding-service:8002/embed
```

### Option 2: Use Both (Hybrid Approach)

- Use local service for initial filtering/bulk processing
- Use OpenAI for final high-quality matching

## Cost Estimation (OpenAI)

If using OpenAI `text-embedding-3-small`:

| News Volume | Cost per Day | Cost per Month |
|-------------|--------------|----------------|
| 1,000 items/day | ~$0.50 | ~$15 |
| 10,000 items/day | ~$5.00 | ~$150 |
| 100,000 items/day | ~$50.00 | ~$1,500 |

*Assumes average 500 tokens per news item*

## Recommendation

**For Development/Testing:** Use local service (Port 8001) - Free, fast
**For Production/Quality:** Use OpenAI service (Port 8002) - Better results

## Test Both Services

```bash
# Test local service
curl -X POST http://localhost:8001/embed \
  -H "Content-Type: application/json" \
  -d '{"text": "Reuters reports market trends"}'

# Test OpenAI service  
curl -X POST http://localhost:8002/embed \
  -H "Content-Type: application/json" \
  -d '{"text": "Reuters reports market trends"}'
```

Compare the embedding quality for your use case!

## Current System Status

Both services are running and healthy:
- ✅ Local Embedding Service: http://localhost:8001 (healthy)
- ✅ OpenAI Embedding Service: http://localhost:8002 (healthy)
- 🔄 Flink: Currently using **local service** (embedding-service:8001)










