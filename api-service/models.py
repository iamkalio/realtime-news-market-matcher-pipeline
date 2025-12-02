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


class StatsResponse(BaseModel):
    total_news: int
    total_processed: int
    processing_rate: float
    latest_news_time: Optional[str]
    latest_processed_time: Optional[str]

