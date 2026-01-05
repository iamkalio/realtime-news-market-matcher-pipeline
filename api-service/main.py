import os
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from kafka_consumer import KafkaDataStore, start_kafka_consumers
from models import NewsItem, ProcessedNews, StatsResponse, Market
from db import get_markets, get_market_stats

# Global data store
data_store = KafkaDataStore(max_size=1000)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup: Start Kafka consumers
    print("Starting Kafka consumers...")
    start_kafka_consumers(data_store)
    yield
    # Shutdown: Cleanup if needed
    print("Shutting down...")


app = FastAPI(
    title="News Processing API",
    description="API to query news stream and processed news from Kafka",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "news-processing-api",
        "endpoints": {
            "news": "/api/news",
            "processed": "/api/processed",
            "markets": "/api/markets",
            "stats": "/api/stats",
            "stream": "/ws",
        },
    }


@app.get("/api/news", response_model=List[NewsItem])
async def get_news(limit: int = 100):
    """Get recent news from news_stream."""
    news = data_store.get_recent_news(limit=limit)
    return news


@app.get("/api/processed", response_model=List[ProcessedNews])
async def get_processed_news(limit: int = 100):
    """Get recent processed news."""
    processed = data_store.get_recent_processed(limit=limit)
    return processed


@app.get("/api/markets", response_model=List[Market])
async def get_markets_endpoint(
    limit: int = Query(100, ge=1, le=1000),
    is_resolved: Optional[bool] = Query(None, description="Filter by resolved status")
):
    """Get list of saved markets from database."""
    markets = get_markets(limit=limit, is_resolved=is_resolved)
    return markets


@app.get("/api/stats", response_model=StatsResponse)
async def get_stats():
    """Get pipeline statistics including market stats."""
    stats = data_store.get_stats()
    market_stats = get_market_stats()
    stats["markets"] = market_stats
    return stats


@app.get("/api/news/source/{source}")
async def get_news_by_source(source: str):
    """Get news by source."""
    news = data_store.get_recent_news(limit=1000)
    filtered = [item for item in news if item.get("source", "").lower() == source.lower()]
    return filtered


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time streaming."""
    await websocket.accept()
    
    try:
        # Send initial stats
        stats = data_store.get_stats()
        await websocket.send_json({"type": "stats", "data": stats})
        
        # Keep connection alive and send updates
        last_news_count = stats["total_news"]
        last_processed_count = stats["total_processed"]
        
        import asyncio
        while True:
            await asyncio.sleep(1)  # Check every second
            
            current_stats = data_store.get_stats()
            
            # Send updates if data changed
            if (
                current_stats["total_news"] > last_news_count
                or current_stats["total_processed"] > last_processed_count
            ):
                await websocket.send_json(
                    {"type": "stats", "data": current_stats}
                )
                
                # Send new news items
                new_news = data_store.get_recent_news(
                    limit=current_stats["total_news"] - last_news_count
                )
                for news in new_news[-10:]:  # Last 10 new news items
                    await websocket.send_json(
                        {"type": "news", "data": news}
                    )
                
                # Send new processed items
                new_processed = data_store.get_recent_processed(
                    limit=current_stats["total_processed"] - last_processed_count
                )
                for processed in new_processed[-10:]:  # Last 10 new processed items
                    await websocket.send_json(
                        {"type": "processed", "data": processed}
                    )
                
                last_news_count = current_stats["total_news"]
                last_processed_count = current_stats["total_processed"]
            
            # Keep-alive ping
            await websocket.send_json({"type": "ping"})
            
    except WebSocketDisconnect:
        print("WebSocket client disconnected")


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)

