from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from typing import List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Embedding Service",
    description="Microservice for generating text embeddings using SentenceTransformers",
    version="1.0.0"
)

# 🧠 Load model ONCE at startup
logger.info("Loading embedding model: all-MiniLM-L6-v2")
model = SentenceTransformer("all-MiniLM-L6-v2")
logger.info("Model loaded successfully")


class EmbedRequest(BaseModel):
    text: str


class EmbedBatchRequest(BaseModel):
    texts: List[str]


class EmbedResponse(BaseModel):
    embedding: List[float]
    dimension: int


class EmbedBatchResponse(BaseModel):
    embeddings: List[List[float]]
    dimension: int
    count: int


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "embedding-service",
        "model": "all-MiniLM-L6-v2",
        "dimension": 384,
        "endpoints": {
            "single": "POST /embed",
            "batch": "POST /embed/batch",
            "health": "GET /health"
        }
    }


@app.get("/health")
async def health():
    """Health check for monitoring."""
    return {"status": "ok", "model_loaded": model is not None}


@app.post("/embed", response_model=EmbedResponse)
async def embed(req: EmbedRequest):
    """
    Generate embedding for a single text.
    
    Args:
        req: EmbedRequest with text field
        
    Returns:
        EmbedResponse with embedding vector and dimension
    """
    try:
        logger.info(f"Generating embedding for text: {req.text[:50]}...")
        vec = model.encode(req.text).tolist()
        return {
            "embedding": vec,
            "dimension": len(vec)
        }
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/embed/batch", response_model=EmbedBatchResponse)
async def embed_batch(req: EmbedBatchRequest):
    """
    Generate embeddings for multiple texts in batch.
    More efficient than calling /embed multiple times.
    
    Args:
        req: EmbedBatchRequest with list of texts
        
    Returns:
        EmbedBatchResponse with list of embedding vectors
    """
    try:
        logger.info(f"Generating embeddings for {len(req.texts)} texts")
        
        if len(req.texts) == 0:
            raise HTTPException(status_code=400, detail="No texts provided")
        
        if len(req.texts) > 100:
            raise HTTPException(
                status_code=400, 
                detail="Maximum 100 texts per batch request"
            )
        
        # Batch encoding is more efficient
        vecs = model.encode(req.texts).tolist()
        
        return {
            "embeddings": vecs,
            "dimension": len(vecs[0]) if vecs else 0,
            "count": len(vecs)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating batch embeddings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

