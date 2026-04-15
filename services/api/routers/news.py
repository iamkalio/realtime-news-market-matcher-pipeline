from typing import List

from fastapi import APIRouter, Request

from models import NewsItem, ProcessedNews

router = APIRouter(prefix="/api")


@router.get("/news", response_model=List[NewsItem])
async def get_news(request: Request, limit: int = 100):
    """Get recent news from news_stream."""
    return request.app.state.data_store.get_recent_news(limit=limit)


@router.get("/processed", response_model=List[ProcessedNews])
async def get_processed_news(request: Request, limit: int = 100):
    """Get recent processed (matched) news."""
    return request.app.state.data_store.get_recent_processed(limit=limit)


@router.get("/news/source/{source}")
async def get_news_by_source(request: Request, source: str):
    """Get news filtered by source."""
    news = request.app.state.data_store.get_recent_news(limit=1000)
    return [item for item in news if item.get("source", "").lower() == source.lower()]
