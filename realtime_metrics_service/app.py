import asyncio
import os
import threading
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from consumer import KinesisConsumer
from metrics_state import MetricsState
from analytics_state import AnalyticsState
from athena_analytics import AthenaAnalyticsClient
from latency_metrics import LatencyMetricsStore
from pyspark_direct_pipeline import PySparkDirectPipeline
from redis_snapshot_store import RedisSnapshotStore, RedisSnapshotSubscriber


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
latency_store = LatencyMetricsStore()

realtime_hub  = WebSocketHub()
analytics_hub = WebSocketHub()
redis_hub = WebSocketHub()
redis_db_hub = WebSocketHub()
pyspark_hub = WebSocketHub()

_broadcast_task: asyncio.Task | None = None
_redis_subscriber: RedisSnapshotSubscriber | None = None
_redis_subscriber_thread: threading.Thread | None = None
_pyspark_pipeline: PySparkDirectPipeline | None = None


async def _broadcast_pipeline(hub: WebSocketHub, pipeline: str, payload: dict):
    payload.setdefault("meta", {})["pipeline"] = pipeline
    latency_store.record(pipeline, payload)
    await hub.broadcast_json(payload)


async def _broadcast_loop():
    """
    Every second:
      1. Broadcast the raw realtime snapshot to /ws
      2. Feed the snapshot into AnalyticsState
      3. Broadcast the analytics snapshot to /ws/analytics
    """
    while True:
        snap = state.snapshot()
        await _broadcast_pipeline(realtime_hub, "realtime-service", snap)

        analytics.ingest(snap)
        analytics_snap = analytics.snapshot()
        await analytics_hub.broadcast_json(analytics_snap)

        redis_db_snap = redis_store.read_snapshot("redis-metrics")
        if redis_db_snap:
            await _broadcast_pipeline(redis_db_hub, "redis-db", redis_db_snap)

        await asyncio.sleep(1.0)


@app.on_event("startup")
async def startup_event():
    global _broadcast_task, _redis_subscriber, _redis_subscriber_thread, _pyspark_pipeline
    loop = asyncio.get_running_loop()
    consumer.start()
    _redis_subscriber = RedisSnapshotSubscriber()

    def on_redis_payload(payload: dict):
        asyncio.run_coroutine_threadsafe(
            _broadcast_pipeline(redis_hub, "redis-pubsub", payload),
            loop,
        )

    _redis_subscriber_thread = threading.Thread(
        target=_redis_subscriber.listen,
        args=("redis-metrics", on_redis_payload),
        daemon=True,
    )
    _redis_subscriber_thread.start()

    if os.getenv("ENABLE_PYSPARK_DIRECT", "true").lower() in {"1", "true", "yes"}:
        interval = float(os.getenv("PYSPARK_DIRECT_INTERVAL_SECONDS", "1.0"))

        def on_pyspark_snapshot(payload: dict):
            asyncio.run_coroutine_threadsafe(
                _broadcast_pipeline(pyspark_hub, "pyspark-direct", payload),
                loop,
            )

        _pyspark_pipeline = PySparkDirectPipeline(on_pyspark_snapshot, interval_seconds=interval)
        _pyspark_pipeline.start()

    _broadcast_task = asyncio.create_task(_broadcast_loop())


@app.on_event("shutdown")
async def shutdown_event():
    global _broadcast_task, _redis_subscriber, _pyspark_pipeline
    consumer.stop()
    if _redis_subscriber is not None:
        _redis_subscriber.stop()
    if _pyspark_pipeline is not None:
        _pyspark_pipeline.stop()
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
        "metrics_redis_db":   "/metrics/redis-db",
        "metrics_pyspark":    "/metrics/pyspark",
        "metrics_latency":    "/metrics/latency",
        "metrics_analytics":  "/metrics/analytics",
        "metrics_realtime_rollup": "/metrics/realtime-rollup",
        "redis":              redis_store.status(),
        "websocket":          "/ws",
        "websocket_redis":    "/ws/redis",
        "websocket_redis_db": "/ws/redis-db",
        "websocket_pyspark":  "/ws/pyspark",
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


@app.get("/metrics/redis-db")
def get_metrics_redis_db():
    return get_metrics_redis()


@app.get("/metrics/pyspark")
def get_metrics_pyspark():
    if _pyspark_pipeline is None:
        return {"error": "pyspark direct pipeline disabled"}
    return _pyspark_pipeline.snapshot()


@app.get("/metrics/latency")
def get_metrics_latency():
    return latency_store.summary()


@app.get("/metrics/latency/recent")
def get_metrics_latency_recent(limit: int = 200):
    return {"samples": latency_store.recent(limit=limit)}


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
    """Push Redis pub/sub pipeline snapshots when available."""
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


@app.websocket("/ws/redis-db")
async def websocket_redis_db(websocket: WebSocket):
    """Push Redis snapshot-store pipeline snapshots after reading Redis as a database."""
    await redis_db_hub.connect(websocket)
    initial = redis_store.read_snapshot("redis-metrics")
    await websocket.send_json(initial or {"error": "redis metrics snapshot unavailable"})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await redis_db_hub.disconnect(websocket)
    except Exception:
        await redis_db_hub.disconnect(websocket)


@app.websocket("/ws/pyspark")
async def websocket_pyspark(websocket: WebSocket):
    """Push PySpark direct pipeline snapshots every second."""
    await pyspark_hub.connect(websocket)
    if _pyspark_pipeline is None:
        await websocket.send_json({"error": "pyspark direct pipeline disabled"})
    else:
        await websocket.send_json(_pyspark_pipeline.snapshot())
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await pyspark_hub.disconnect(websocket)
    except Exception:
        await pyspark_hub.disconnect(websocket)
