import asyncio
from pathlib import Path
 
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
 
from consumer import KinesisConsumer
from metrics_state import MetricsState
from analytics_state import AnalyticsState
 
 
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
 
state     = MetricsState()
analytics = AnalyticsState()
consumer  = KinesisConsumer(state)
 
realtime_hub  = WebSocketHub()
analytics_hub = WebSocketHub()
 
_broadcast_task: asyncio.Task | None = None
 
 
async def _broadcast_loop():
    """
    Every second:
      1. Broadcast the raw realtime snapshot to /ws
      2. Feed the snapshot into AnalyticsState
      3. Broadcast the analytics snapshot to /ws/analytics
    """
    while True:
        snap = state.snapshot()
 
        await realtime_hub.broadcast_json(snap)
 
        analytics.ingest(snap)
        await analytics_hub.broadcast_json(analytics.snapshot())
 
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
        "service":            "realtime-metrics-service",
        "dashboard":          "/dashboard",
        "metrics":            "/metrics",
        "metrics_analytics":  "/metrics/analytics",
        "websocket":          "/ws",
        "websocket_analytics": "/ws/analytics",
    }
 
 
@app.get("/metrics")
def get_metrics():
    return state.snapshot()
 
 
@app.get("/metrics/analytics")
def get_metrics_analytics():
    return analytics.snapshot()
 
 
@app.get("/dashboard")
def get_dashboard():
    base_dir = Path(__file__).resolve().parent
    dashboard_path = base_dir / "static" / "intex.html"
    if not dashboard_path.exists():
        dashboard_path = base_dir / "static" / "index.html"
    return FileResponse(dashboard_path)
 
  
@app.websocket("/ws")
async def websocket_realtime(websocket: WebSocket):
    """Push realtime snapshot every second."""
    await realtime_hub.connect(websocket)
    await websocket.send_json(state.snapshot())
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await realtime_hub.disconnect(websocket)
    except Exception:
        await realtime_hub.disconnect(websocket)
 
 
@app.websocket("/ws/analytics")
async def websocket_analytics(websocket: WebSocket):
    """Push analytics snapshot every second."""
    await analytics_hub.connect(websocket)
    await websocket.send_json(analytics.snapshot())
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await analytics_hub.disconnect(websocket)
    except Exception:
        await analytics_hub.disconnect(websocket)
 
