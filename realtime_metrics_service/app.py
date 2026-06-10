import asyncio
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from consumer import KinesisConsumer
from metrics_state import MetricsState
from analytics_state import AnalyticsState
from athena_analytics import AthenaAnalyticsClient
from redis_snapshot_store import RedisSnapshotStore


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
athena_analytics = AthenaAnalyticsClient()
consumer  = KinesisConsumer(state)
redis_store = RedisSnapshotStore()

realtime_hub  = WebSocketHub()
analytics_hub = WebSocketHub()
redis_hub = WebSocketHub()

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
        snap.setdefault("meta", {})["pipeline"] = "realtime-service"

        await realtime_hub.broadcast_json(snap)

        analytics.ingest(snap)
        analytics_snap = analytics.snapshot()
        await analytics_hub.broadcast_json(analytics_snap)

        redis_snap = redis_store.read_snapshot("redis-metrics")
        if redis_snap:
            await redis_hub.broadcast_json(redis_snap)

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


# ── REST endpoints ────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "service":            "realtime-metrics-service",
        "dashboard":          "/dashboard",
        "metrics":            "/metrics",
        "metrics_redis":      "/metrics/redis",
        "metrics_analytics":  "/metrics/analytics",
        "metrics_realtime_rollup": "/metrics/realtime-rollup",
        "redis":              redis_store.status(),
        "websocket":          "/ws",
        "websocket_redis":    "/ws/redis",
        "websocket_realtime_rollup": "/ws/realtime-rollup",
    }


@app.get("/metrics")
def get_metrics():
    snap = state.snapshot()
    snap.setdefault("meta", {})["pipeline"] = "realtime-service"
    return snap


@app.get("/metrics/redis")
def get_metrics_redis():
    snap = redis_store.read_snapshot("redis-metrics")
    if snap is None:
        return {
            "error": "redis metrics snapshot unavailable",
            "redis": redis_store.status(),
        }
    return snap


@app.get("/metrics/analytics")
def get_metrics_analytics():
    return athena_analytics.snapshot()


@app.get("/metrics/realtime-rollup")
def get_metrics_realtime_rollup():
    return analytics.snapshot()


@app.get("/health/redis")
def get_redis_health():
    return redis_store.status()


@app.get("/dashboard")
def get_dashboard():
    base_dir = Path(__file__).resolve().parent
    dashboard_path = base_dir / "static" / "index.html"
    if not dashboard_path.exists():
        dashboard_path = base_dir / "static" / "intex.html"
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


@app.websocket("/ws/realtime-rollup")
async def websocket_analytics(websocket: WebSocket):
    """Push in-memory realtime rollup snapshot every second."""
    await analytics_hub.connect(websocket)
    await websocket.send_json(analytics.snapshot())
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await analytics_hub.disconnect(websocket)
    except Exception:
        await analytics_hub.disconnect(websocket)


@app.websocket("/ws/redis")
async def websocket_redis(websocket: WebSocket):
    """Push Redis pipeline snapshot when available."""
    await redis_hub.connect(websocket)
    initial = redis_store.read_snapshot("redis-metrics")
    await websocket.send_json(initial or {"error": "redis metrics snapshot unavailable"})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await redis_hub.disconnect(websocket)
    except Exception:
        await redis_hub.disconnect(websocket)
