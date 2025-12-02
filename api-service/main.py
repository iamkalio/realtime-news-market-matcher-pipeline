import os
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from kafka_consumer import KafkaDataStore, start_kafka_consumers
from models import FraudAlert, StatsResponse, Transaction

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
    title="Fraud Detection API",
    description="API to query transactions and fraud alerts from Kafka",
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
        "service": "fraud-detection-api",
        "endpoints": {
            "transactions": "/api/transactions",
            "fraud-alerts": "/api/fraud-alerts",
            "stats": "/api/stats",
            "stream": "/ws",
        },
    }


@app.get("/api/transactions", response_model=List[Transaction])
async def get_transactions(limit: int = 100):
    """Get recent transactions."""
    transactions = data_store.get_recent_transactions(limit=limit)
    return transactions


@app.get("/api/fraud-alerts", response_model=List[FraudAlert])
async def get_fraud_alerts(limit: int = 100):
    """Get recent fraud alerts."""
    alerts = data_store.get_recent_alerts(limit=limit)
    return alerts


@app.get("/api/stats", response_model=StatsResponse)
async def get_stats():
    """Get pipeline statistics."""
    stats = data_store.get_stats()
    return stats


@app.get("/api/transactions/{transaction_id}")
async def get_transaction_by_id(transaction_id: str):
    """Get a specific transaction by ID."""
    transactions = data_store.get_recent_transactions(limit=1000)
    for tx in transactions:
        if tx["transaction_id"] == transaction_id:
            return tx
    return JSONResponse(
        status_code=404, content={"error": "Transaction not found"}
    )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time streaming."""
    await websocket.accept()
    
    try:
        # Send initial stats
        stats = data_store.get_stats()
        await websocket.send_json({"type": "stats", "data": stats})
        
        # Keep connection alive and send updates
        last_tx_count = stats["total_transactions"]
        last_alert_count = stats["total_fraud_alerts"]
        
        import asyncio
        while True:
            await asyncio.sleep(1)  # Check every second
            
            current_stats = data_store.get_stats()
            
            # Send updates if data changed
            if (
                current_stats["total_transactions"] > last_tx_count
                or current_stats["total_fraud_alerts"] > last_alert_count
            ):
                await websocket.send_json(
                    {"type": "stats", "data": current_stats}
                )
                
                # Send new transactions
                new_tx = data_store.get_recent_transactions(
                    limit=current_stats["total_transactions"] - last_tx_count
                )
                for tx in new_tx[-10:]:  # Last 10 new transactions
                    await websocket.send_json(
                        {"type": "transaction", "data": tx}
                    )
                
                # Send new alerts
                new_alerts = data_store.get_recent_alerts(
                    limit=current_stats["total_fraud_alerts"] - last_alert_count
                )
                for alert in new_alerts[-10:]:  # Last 10 new alerts
                    await websocket.send_json(
                        {"type": "fraud_alert", "data": alert}
                    )
                
                last_tx_count = current_stats["total_transactions"]
                last_alert_count = current_stats["total_fraud_alerts"]
            
            # Keep-alive ping
            await websocket.send_json({"type": "ping"})
            
    except WebSocketDisconnect:
        print("WebSocket client disconnected")


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)

