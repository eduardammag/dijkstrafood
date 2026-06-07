from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Any

_MAX_BUCKETS = 300


@dataclass
class _Bucket:
    ts: float
    orders_placed:     int   = 0
    orders_delivered:  int   = 0
    orders_cancelled:  int   = 0
    revenue_delta:     float = 0.0
    # point-in-time readings
    avg_delivery_ms:   float = 0.0
    active_couriers:   int   = 0
    active_orders:     int   = 0


class AnalyticsState:

    def __init__(self) -> None:
        self._lock    = Lock()
        self._buckets: deque[_Bucket] = deque(maxlen=_MAX_BUCKETS)
        self._prev:   dict[str, Any] | None = None

        # Lifetime totals (never reset)
        self._lt_orders_placed:    int   = 0
        self._lt_orders_delivered: int   = 0
        self._lt_orders_cancelled: int   = 0
        self._lt_revenue:          float = 0.0


    def ingest(self, snap: dict[str, Any]) -> None:
        """Call once per second with MetricsState.snapshot()."""
        with self._lock:
            prev = self._prev or {}

            def _delta(key: str) -> int | float:
                cur = snap.get(key) or 0
                pre = prev.get(key) or 0
                return max(cur - pre, 0)

            b = _Bucket(
                ts                = time.time(),
                orders_placed     = int(_delta("orders_total")),
                orders_delivered  = int(_delta("orders_delivered")),
                orders_cancelled  = int(_delta("orders_cancelled")),
                revenue_delta     = float(_delta("total_revenue")),
                avg_delivery_ms   = float(snap.get("avg_delivery_time_ms") or 0),
                active_couriers   = int(snap.get("active_couriers") or 0),
                active_orders     = int(snap.get("active_orders") or 0),
            )
            self._buckets.append(b)

            self._lt_orders_placed    += b.orders_placed
            self._lt_orders_delivered += b.orders_delivered
            self._lt_orders_cancelled += b.orders_cancelled
            self._lt_revenue          += b.revenue_delta

            self._prev = dict(snap)
            

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            buckets = list(self._buckets)
            lt = {
                "orders_placed":    self._lt_orders_placed,
                "orders_delivered": self._lt_orders_delivered,
                "orders_cancelled": self._lt_orders_cancelled,
                "revenue":          round(self._lt_revenue, 2),
            }

        if not buckets:
            return _empty(lt)

        now = time.time()
        w60  = [b for b in buckets if now - b.ts <= 60]
        w300 = buckets  # full window (~5 min)

        def _s(attr: str, w: list) -> float:
            return sum(getattr(b, attr) for b in w)

        last = buckets[-1]

        placed_60   = int(_s("orders_placed",    w60))
        del_60      = int(_s("orders_delivered", w60))
        can_60      = int(_s("orders_cancelled", w60))
        rev_60      = round(_s("revenue_delta",  w60), 2)
        rev_300     = round(_s("revenue_delta",  w300), 2)

        delivery_rate = round(del_60 / max(placed_60, 1) * 100, 1)
        cancel_rate   = round(can_60 / max(placed_60, 1) * 100, 1)
        avg_del_s     = round(last.avg_delivery_ms / 1000, 2)

        def _series(attr: str, w: list, scale: float = 1.0) -> list:
            return [
                {"ts": round(b.ts * 1000), "v": round(getattr(b, attr) * scale, 3)}
                for b in w
            ]

        rev_cum: list = []
        running = 0.0
        for b in w300:
            running += b.revenue_delta
            rev_cum.append({"ts": round(b.ts * 1000), "v": round(running, 2)})

        return {
            "kpis": {
                "orders_last_60s":     placed_60,
                "revenue_last_60s":    rev_60,
                "revenue_last_5m":     rev_300,
                "delivery_rate_pct":   delivery_rate,
                "cancel_rate_pct":     cancel_rate,
                "avg_delivery_time_s": avg_del_s,
                "active_couriers":     last.active_couriers,
                "active_orders":       last.active_orders,
            },
            "lifetime": lt,
            "series": {
                "throughput":    _series("orders_placed",    buckets[-60:]),
                "deliveries":    _series("orders_delivered", buckets[-60:]),
                "couriers":      _series("active_couriers",  buckets[-60:]),
                "active_orders": _series("active_orders",    buckets[-60:]),
                "delivery_time": _series("avg_delivery_ms",  buckets[-60:], scale=1/1000),
                "revenue_cum":   rev_cum,
            },
            "window_s": len(w300),
        }


def _empty(lt: dict) -> dict:
    return {
        "kpis": {
            "orders_last_60s": 0, "revenue_last_60s": 0.0,
            "revenue_last_5m": 0.0, "delivery_rate_pct": 0.0,
            "cancel_rate_pct": 0.0, "avg_delivery_time_s": 0.0,
            "active_couriers": 0, "active_orders": 0,
        },
        "lifetime": lt,
        "series": {
            "throughput": [], "deliveries": [], "couriers": [],
            "active_orders": [], "delivery_time": [], "revenue_cum": [],
        },
        "window_s": 0,
    }