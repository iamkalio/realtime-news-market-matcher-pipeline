import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import logging
from openai import OpenAI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="OpenAI Embedding Service",
    description="Microservice for generating text embeddings using OpenAI API",
    version="1.0.0"
)

# Get OpenAI API key from environment
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY environment variable not set!")
    raise ValueError("OPENAI_API_KEY must be set")

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)

# Model configuration
EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
logger.info(f"Using OpenAI model: {EMBEDDING_MODEL}")


class EmbedRequest(BaseModel):
    text: str


class EmbedBatchRequest(BaseModel):
    texts: List[str]


class EmbedResponse(BaseModel):
    embedding: List[float]
    dimension: int
    model: str


class EmbedBatchResponse(BaseModel):
    embeddings: List[List[float]]
    dimension: int
    count: int
    model: str


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "openai-embedding-service",
        "model": EMBEDDING_MODEL,
        "dimension": 1536 if "3-small" in EMBEDDING_MODEL else 3072,
        "endpoints": {
            "single": "POST /embed",
            "batch": "POST /embed/batch",
            "health": "GET /health"
        }
    }


@app.get("/health")
async def health():
    """Health check for monitoring."""
    return {
        "status": "ok",
        "api_key_configured": bool(OPENAI_API_KEY),
        "model": EMBEDDING_MODEL
    }


@app.post("/embed", response_model=EmbedResponse)
async def embed(req: EmbedRequest):
    """
    Generate embedding for a single text using OpenAI API.
    
    Args:
        req: EmbedRequest with text field
        
    Returns:
        EmbedResponse with embedding vector, dimension, and model
    """
    try:
        logger.info(f"Generating OpenAI embedding for text: {req.text[:50]}...")
        
        response = client.embeddings.create(
            input=req.text,
            model=EMBEDDING_MODEL
        )
        
        embedding = response.data[0].embedding
        
        return {
            "embedding": embedding,
            "dimension": len(embedding),
            "model": response.model
        }
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/embed/batch", response_model=EmbedBatchResponse)
async def embed_batch(req: EmbedBatchRequest):
    """
    Generate embeddings for multiple texts in batch using OpenAI API.
    More efficient than calling /embed multiple times.
    
    Args:
        req: EmbedBatchRequest with list of texts
        
    Returns:
        EmbedBatchResponse with list of embedding vectors
    """
    try:
        logger.info(f"Generating OpenAI embeddings for {len(req.texts)} texts")
        
        if len(req.texts) == 0:
            raise HTTPException(status_code=400, detail="No texts provided")
        
        # OpenAI API supports batch processing natively
        response = client.embeddings.create(
            input=req.texts,
            model=EMBEDDING_MODEL
        )
        
        embeddings = [item.embedding for item in response.data]
        
        return {
            "embeddings": embeddings,
            "dimension": len(embeddings[0]) if embeddings else 0,
            "count": len(embeddings),
            "model": response.model
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating batch embeddings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)




















