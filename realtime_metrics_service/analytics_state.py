from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Any

_MAX_BUCKETS = 300  # 5 minutos de histórico (1 bucket/s)


@dataclass
class _Bucket:
    ts: float
    # deltas (quanto mudou desde o snapshot anterior)
    new_orders: int = 0       # delta de total_orders_processed
    new_delivered: int = 0    # delta de orders_delivered
    new_created: int = 0      # delta via orders_created_per_minute (snapshot direto)
    # leituras point-in-time
    orders_preparing: int = 0
    orders_waiting_courier: int = 0
    orders_delivering: int = 0
    orders_delivered: int = 0
    couriers_available: int = 0
    latency_avg_ms: float = 0.0
    latency_last_ms: float = 0.0
    created_per_min: int = 0
    total_processed: int = 0


class AnalyticsState:
    """Acumulador thread-safe de métricas analíticas."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._buckets: deque[_Bucket] = deque(maxlen=_MAX_BUCKETS)
        self._prev: dict[str, Any] | None = None

    # Ingestão

    def ingest(self, snap: dict[str, Any]) -> None:
        """Chamar uma vez por segundo com MetricsState.snapshot()."""
        with self._lock:
            prev = self._prev or {}

            def _delta(key: str) -> int:
                return max(int(snap.get(key) or 0) - int(prev.get(key) or 0), 0)

            b = _Bucket(
                ts=time.time(),
                new_orders          = _delta("total_orders_processed"),
                new_delivered       = _delta("orders_delivered"),
                orders_preparing    = int(snap.get("orders_preparing") or 0),
                orders_waiting_courier = int(snap.get("orders_waiting_courier") or 0),
                orders_delivering   = int(snap.get("orders_delivering") or 0),
                orders_delivered    = int(snap.get("orders_delivered") or 0),
                couriers_available  = int(snap.get("couriers_available") or 0),
                latency_avg_ms      = float(snap.get("event_to_consumer_latency_ms_avg_1m") or 0),
                latency_last_ms     = float(snap.get("event_to_consumer_latency_ms_last") or 0),
                created_per_min     = int(snap.get("orders_created_per_minute") or 0),
                total_processed     = int(snap.get("total_orders_processed") or 0),
            )
            self._buckets.append(b)
            self._prev = dict(snap)

    # Snapshot para o frontend

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            buckets = list(self._buckets)

        if not buckets:
            return _empty()

        now = time.time()
        w60  = [b for b in buckets if now - b.ts <= 60]
        w300 = buckets

        def _s(attr: str, w: list) -> float:
            return sum(getattr(b, attr) for b in w)

        last = buckets[-1]

        # KPIs
        new_orders_60    = int(_s("new_orders",    w60))
        new_delivered_60 = int(_s("new_delivered", w60))
        throughput_60    = round(new_orders_60 / 60, 2)   # pedidos/s médio

        # Séries temporais (último minuto)
        def _ser(attr: str, w: list) -> list[dict]:
            return [{"ts": round(b.ts * 1000), "v": getattr(b, attr)} for b in w]

        # Série de latência média (janela 60 s)
        latency_series = _ser("latency_avg_ms", buckets[-60:])

        # Série de pedidos por minuto (valor do snapshot, não delta)
        created_series = _ser("created_per_min", buckets[-60:])

        # Série de pedidos por status
        preparing_series  = _ser("orders_preparing",       buckets[-60:])
        delivering_series = _ser("orders_delivering",       buckets[-60:])
        delivered_series  = _ser("orders_delivered",        buckets[-60:])
        couriers_series   = _ser("couriers_available",      buckets[-60:])

        # Série de novos pedidos processados por bucket (delta/s)
        throughput_series = _ser("new_orders", buckets[-60:])

        # Série acumulada de entregas dentro da janela 5 min
        delivered_cum: list[dict] = []
        running = 0
        for b in w300:
            running += b.new_delivered
            delivered_cum.append({"ts": round(b.ts * 1000), "v": running})

        return {
            "kpis": {
                "new_orders_last_60s":    new_orders_60,
                "new_delivered_last_60s": new_delivered_60,
                "throughput_per_s":       throughput_60,
                "orders_preparing":       last.orders_preparing,
                "orders_waiting_courier": last.orders_waiting_courier,
                "orders_delivering":      last.orders_delivering,
                "orders_delivered_total": last.orders_delivered,
                "couriers_available":     last.couriers_available,
                "latency_avg_1m_ms":      last.latency_avg_ms,
                "latency_last_ms":        last.latency_last_ms,
                "created_per_min":        last.created_per_min,
                "total_processed":        last.total_processed,
            },
            "series": {
                "throughput":  throughput_series,
                "created":     created_series,
                "preparing":   preparing_series,
                "delivering":  delivering_series,
                "delivered":   delivered_series,
                "couriers":    couriers_series,
                "latency":     latency_series,
                "delivered_cum": delivered_cum,
            },
            "window_s": len(w300),
        }


def _empty() -> dict:
    return {
        "kpis": {
            "new_orders_last_60s": 0, "new_delivered_last_60s": 0,
            "throughput_per_s": 0.0, "orders_preparing": 0,
            "orders_waiting_courier": 0, "orders_delivering": 0,
            "orders_delivered_total": 0, "couriers_available": 0,
            "latency_avg_1m_ms": 0.0, "latency_last_ms": 0.0,
            "created_per_min": 0, "total_processed": 0,
        },
        "series": {
            "throughput": [], "created": [], "preparing": [],
            "delivering": [], "delivered": [], "couriers": [],
            "latency": [], "delivered_cum": [],
        },
        "window_s": 0,
    }