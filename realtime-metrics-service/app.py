import asyncio
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from consumer import KinesisConsumer
from metrics_state import MetricsState


class WebSocketHub:
    def __init__(self):
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            self._connections.discard(websocket)

    async def broadcast_json(self, payload: dict):
        async with self._lock:
            connections = list(self._connections)

        stale: list[WebSocket] = []
        for conn in connections:
            try:
                await conn.send_json(payload)
            except Exception:
                stale.append(conn)

        if stale:
            async with self._lock:
                for conn in stale:
                    self._connections.discard(conn)


app = FastAPI(title="Realtime Metrics Service")
state = MetricsState()
consumer = KinesisConsumer(state)
hub = WebSocketHub()
_broadcast_task: asyncio.Task | None = None


async def _broadcast_loop():
    while True:
        await hub.broadcast_json(state.snapshot())
        await asyncio.sleep(1.0)


@app.on_event("startup")
async def startup_event():
    global _broadcast_task
    consumer.start()
    _broadcast_task = asyncio.create_task(_broadcast_loop())


@app.on_event("shutdown")
async def shutdown_event():
    global _broadcast_task
    consumer.stop()
    if _broadcast_task is not None:
        _broadcast_task.cancel()
        try:
            await _broadcast_task
        except asyncio.CancelledError:
            pass


@app.get("/")
def root():
    return {
        "service": "realtime-metrics-service",
        "dashboard": "/dashboard",
        "metrics": "/metrics",
        "websocket": "/ws",
    }


@app.get("/metrics")
def get_metrics():
    return state.snapshot()


@app.get("/dashboard")
def get_dashboard():
    base_dir = Path(__file__).resolve().parent
    return FileResponse(base_dir / "static" / "index.html")


@app.websocket("/ws")
async def websocket_metrics(websocket: WebSocket):
    await hub.connect(websocket)
    await websocket.send_json(state.snapshot())

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await hub.disconnect(websocket)
    except Exception:
        await hub.disconnect(websocket)
