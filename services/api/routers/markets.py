from typing import List, Optional

from fastapi import APIRouter, Query, Request

from db import get_markets, get_market_stats
from models import Market, StatsResponse

router = APIRouter(prefix="/api")


@router.get("/markets", response_model=List[Market])
async def get_markets_endpoint(
    limit: int = Query(100, ge=1, le=1000),
    is_resolved: Optional[bool] = Query(None, description="Filter by resolved status"),
):
    """Get markets from database."""
    return get_markets(limit=limit, is_resolved=is_resolved)


@router.get("/stats", response_model=StatsResponse)
async def get_stats(request: Request):
    """Get pipeline statistics including market stats."""
    stats = request.app.state.data_store.get_stats()
    stats["markets"] = get_market_stats()
    return stats
