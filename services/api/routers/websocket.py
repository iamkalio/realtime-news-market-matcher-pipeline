import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time streaming."""
    await websocket.accept()

    try:
        data_store = websocket.app.state.data_store
        stats = data_store.get_stats()
        await websocket.send_json({"type": "stats", "data": stats})

        last_news_count = stats["total_news"]
        last_processed_count = stats["total_processed"]

        while True:
            await asyncio.sleep(1)
            current_stats = data_store.get_stats()

            if (
                current_stats["total_news"] > last_news_count
                or current_stats["total_processed"] > last_processed_count
            ):
                await websocket.send_json({"type": "stats", "data": current_stats})

                new_news = data_store.get_recent_news(
                    limit=current_stats["total_news"] - last_news_count
                )
                for item in new_news[-10:]:
                    await websocket.send_json({"type": "news", "data": item})

                new_processed = data_store.get_recent_processed(
                    limit=current_stats["total_processed"] - last_processed_count
                )
                for item in new_processed[-10:]:
                    await websocket.send_json({"type": "processed", "data": item})

                last_news_count = current_stats["total_news"]
                last_processed_count = current_stats["total_processed"]

            await websocket.send_json({"type": "ping"})

    except WebSocketDisconnect:
        pass
