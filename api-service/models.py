from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class NewsItem(BaseModel):
    source: str
    date: str
    time: str
    news: str


class ProcessedNews(BaseModel):
    source: str
    date: str
    time: str
    news: str
    matched_market: Optional[str]
    matched_market_id: Optional[int] = None
    matched_market_question: Optional[str] = None
    match_score: Optional[float] = None


class Market(BaseModel):
    id: int
    source: str
    slug: str
    question: str
    is_resolved: bool
    created_at: str
    updated_at: str


class StatsResponse(BaseModel):
    total_news: int
    total_processed: int
    processing_rate: float
    latest_news_time: Optional[str]
    latest_processed_time: Optional[str]
    markets: dict

