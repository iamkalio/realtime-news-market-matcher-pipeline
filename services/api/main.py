import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from consumers.kafka import KafkaDataStore, start_kafka_consumers
from routers import markets, news, websocket


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.data_store = KafkaDataStore(max_size=1000)
    start_kafka_consumers(app.state.data_store)
    yield


app = FastAPI(
    title="News Processing API",
    description="API to query news stream and processed news from Kafka",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(news.router)
app.include_router(markets.router)
app.include_router(websocket.router)


@app.get("/")
async def root():
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


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
