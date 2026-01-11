import logging
import os
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration constants
MODEL_NAME = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
EMBEDDING_DIMENSION = 384
MAX_BATCH_SIZE = 100
LOG_TEXT_PREVIEW_LENGTH = 50

# Global model instance (loaded in lifespan)
model: SentenceTransformer | None = None


class EmbedRequest(BaseModel):
    text: str = Field(..., min_length=1, description="Text to generate embedding for")


class EmbedBatchRequest(BaseModel):
    texts: List[str] = Field(..., min_items=1, description="List of texts to embed")


class EmbedResponse(BaseModel):
    embedding: List[float]
    dimension: int


class EmbedBatchResponse(BaseModel):
    embeddings: List[List[float]]
    dimension: int
    count: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage model lifecycle: load on startup, cleanup on shutdown."""
    global model
    
    logger.info(f"Loading embedding model: {MODEL_NAME}")
    try:
        model = SentenceTransformer(MODEL_NAME)
        logger.info(f"Model '{MODEL_NAME}' loaded successfully (dimension: {EMBEDDING_DIMENSION})")
    except Exception as e:
        logger.error(f"Failed to load model '{MODEL_NAME}': {e}", exc_info=True)
        raise
    
    yield
    
    logger.info("Shutting down embedding service")


app = FastAPI(
    title="Embedding Service",
    description="Microservice for generating text embeddings using SentenceTransformers",
    version="1.0.0",
    lifespan=lifespan,
)


def _validate_model_loaded() -> None:
    """Ensure the model is loaded before processing requests."""
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Embedding model not loaded. Service is not ready."
        )


@app.get("/")
async def root():
    """Health check endpoint with service information."""
    return {
        "status": "healthy",
        "service": "embedding-service",
        "model": MODEL_NAME,
        "dimension": EMBEDDING_DIMENSION,
        "endpoints": {
            "single": "POST /embed",
            "batch": "POST /embed/batch",
            "health": "GET /health"
        }
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    model_status = model is not None
    return {
        "status": "ok" if model_status else "unhealthy",
        "model_loaded": model_status
    }


@app.post("/embed", response_model=EmbedResponse)
async def embed(req: EmbedRequest) -> EmbedResponse:
    """Generate embedding for a single text.
    
    Args:
        req: EmbedRequest containing the text to embed
        
    Returns:
        EmbedResponse with embedding vector and dimension
        
    Raises:
        HTTPException: If model is not loaded or embedding generation fails
    """
    _validate_model_loaded()
    
    try:
        text_preview = (
            req.text[:LOG_TEXT_PREVIEW_LENGTH] + "..."
            if len(req.text) > LOG_TEXT_PREVIEW_LENGTH
            else req.text
        )
        logger.debug(f"Generating embedding for text: {text_preview}")
        
        embedding_vector = model.encode(req.text).tolist()
        
        return EmbedResponse(
            embedding=embedding_vector,
            dimension=len(embedding_vector)
        )
    except Exception as e:
        logger.error(f"Error generating embedding: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate embedding: {str(e)}"
        )


@app.post("/embed/batch", response_model=EmbedBatchResponse)
async def embed_batch(req: EmbedBatchRequest) -> EmbedBatchResponse:
    """Generate embeddings for multiple texts in batch.
    
    Batch processing is more efficient than calling /embed multiple times
    as it processes all texts in a single model forward pass.
    
    Args:
        req: EmbedBatchRequest containing list of texts to embed
        
    Returns:
        EmbedBatchResponse with list of embedding vectors
        
    Raises:
        HTTPException: If batch size exceeds limit, model not loaded, or processing fails
    """
    _validate_model_loaded()
    
    if len(req.texts) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Batch size {len(req.texts)} exceeds maximum of {MAX_BATCH_SIZE}"
        )
    
    try:
        logger.info(f"Generating embeddings for batch of {len(req.texts)} texts")
        
        embeddings_list = model.encode(req.texts).tolist()
        
        dimension = len(embeddings_list[0]) if embeddings_list else 0
        
        return EmbedBatchResponse(
            embeddings=embeddings_list,
            dimension=dimension,
            count=len(embeddings_list)
        )
    except Exception as e:
        logger.error(f"Error generating batch embeddings: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate batch embeddings: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
