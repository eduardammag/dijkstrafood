from __future__ import annotations

import time
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any

_MAX_BUCKETS = 300  # 5 minutos de historico (1 bucket/s)


@dataclass
class _Bucket:
    ts: float
    new_events: int = 0
    new_orders: int = 0
    new_delivered: int = 0
    orders_preparing: int = 0
    orders_waiting_courier: int = 0
    orders_delivering: int = 0
    orders_delivered: int = 0
    orders_cancelled: int = 0
    orders_open: int = 0
    couriers_available: int = 0
    latency_avg_ms: float = 0.0
    latency_last_ms: float = 0.0
    created_per_min: int = 0
    total_processed: int = 0
    total_events_processed: int = 0


class AnalyticsState:
    """Acumulador thread-safe de metricas analiticas em memoria."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._buckets: deque[_Bucket] = deque(maxlen=_MAX_BUCKETS)
        self._prev: dict[str, Any] | None = None
        self._order_first_seen_at: dict[int, float] = {}
        self._order_delivered_at: dict[int, float] = {}
        self._order_current_status: dict[int, str] = {}
        self._order_status_started_at: dict[int, float] = {}
        self._status_duration_totals_s: Counter[str] = Counter()
        self._status_duration_counts: Counter[str] = Counter()
        self._event_counts_by_hour: Counter[int] = Counter()
        self._order_counts_by_hour: Counter[int] = Counter()
        self._heatmap_counts: Counter[tuple[int, str, int]] = Counter()

    def ingest(self, snap: dict[str, Any]) -> None:
        with self._lock:
            prev = self._prev or {}

            def _delta(key: str) -> int:
                return max(int(snap.get(key) or 0) - int(prev.get(key) or 0), 0)

            bucket = _Bucket(
                ts=time.time(),
                new_events=_delta("total_events_processed"),
                new_orders=_delta("total_orders_processed"),
                new_delivered=_delta("orders_delivered"),
                orders_preparing=int(snap.get("orders_preparing") or 0),
                orders_waiting_courier=int(snap.get("orders_waiting_courier") or 0),
                orders_delivering=int(snap.get("orders_delivering") or 0),
                orders_delivered=int(snap.get("orders_delivered") or 0),
                orders_cancelled=int(snap.get("orders_cancelled") or 0),
                orders_open=int(snap.get("orders_open") or snap.get("active_orders") or 0),
                couriers_available=int(snap.get("couriers_available") or 0),
                latency_avg_ms=float(snap.get("event_to_consumer_latency_ms_avg_1m") or 0),
                latency_last_ms=float(snap.get("event_to_consumer_latency_ms_last") or 0),
                created_per_min=int(snap.get("orders_created_per_minute") or 0),
                total_processed=int(snap.get("total_orders_processed") or 0),
                total_events_processed=int(snap.get("total_events_processed") or 0),
            )
            self._buckets.append(bucket)
            self._prev = dict(snap)

    def ingest_event(self, event: Any) -> None:
        ts = float(getattr(event, "timestamp", 0.0) or time.time())
        order_id = getattr(event, "order_id", None)
        status = (getattr(event, "status", None) or "").upper().strip() or None

        with self._lock:
            dt = datetime.fromtimestamp(ts, timezone.utc)
            hour = dt.hour
            self._event_counts_by_hour[hour] += 1

            if order_id is not None and order_id not in self._order_first_seen_at:
                self._order_first_seen_at[order_id] = ts
                self._order_counts_by_hour[hour] += 1
                dow_num = dt.isoweekday()
                self._heatmap_counts[(dow_num, _dow_label(dow_num), hour)] += 1

            if order_id is None or status is None:
                return

            prev_status = self._order_current_status.get(order_id)
            prev_started_at = self._order_status_started_at.get(order_id)
            if prev_status and prev_started_at is not None and prev_status != status and ts >= prev_started_at:
                self._status_duration_totals_s[prev_status] += ts - prev_started_at
                self._status_duration_counts[prev_status] += 1

            if prev_status != status:
                self._order_current_status[order_id] = status
                self._order_status_started_at[order_id] = ts

            if status == "DELIVERED":
                self._order_delivered_at[order_id] = ts

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            buckets = list(self._buckets)
            latest_snap = dict(self._prev or {})
            current_status = latest_snap.get("current_status") or latest_snap.get("by_status") or []
            by_type = latest_snap.get("by_type") or []
            avg_time_by_status = [
                {
                    "label": status,
                    "value": round(self._status_duration_totals_s[status] / self._status_duration_counts[status], 1),
                    "transitions": int(self._status_duration_counts[status]),
                }
                for status in self._status_duration_totals_s
                if self._status_duration_counts[status] > 0
            ]
            avg_time_by_status.sort(key=lambda row: row["value"], reverse=True)
            events_by_hour = [
                {
                    "hour": hour,
                    "total_events": int(self._event_counts_by_hour[hour]),
                    "total_orders": int(self._order_counts_by_hour[hour]),
                }
                for hour in sorted(self._event_counts_by_hour)
            ]
            demand_heatmap = [
                {
                    "day_of_week": dow_label,
                    "day_of_week_num": dow_num,
                    "hour_utc": hour,
                    "total_orders": int(total_orders),
                }
                for (dow_num, dow_label, hour), total_orders in sorted(self._heatmap_counts.items())
            ]
            delivery_time_histogram = _build_delivery_histogram(
                self._order_first_seen_at,
                self._order_delivered_at,
            )

        if not buckets:
            return _empty(
                current_status=current_status,
                by_type=by_type,
                events_by_hour=events_by_hour,
                avg_time_by_status=avg_time_by_status,
                demand_heatmap=demand_heatmap,
                delivery_time_histogram=delivery_time_histogram,
            )

        now = time.time()
        w60 = [b for b in buckets if now - b.ts <= 60]
        w300 = buckets
        last = buckets[-1]

        def _sum(attr: str, window: list[_Bucket]) -> float:
            return sum(getattr(b, attr) for b in window)

        def _series(attr: str, window: list[_Bucket]) -> list[dict[str, float | int]]:
            return [{"ts": round(b.ts * 1000), "v": getattr(b, attr)} for b in window]

        new_events_60 = int(_sum("new_events", w60))
        new_orders_60 = int(_sum("new_orders", w60))
        new_delivered_60 = int(_sum("new_delivered", w60))
        event_rate_60 = round(new_events_60 / 60, 2)
        throughput_60 = round(new_orders_60 / 60, 2)
        delivery_rate_pct = round((last.orders_delivered / last.total_processed) * 100, 1) if last.total_processed else 0.0
        cancel_rate_pct = round((last.orders_cancelled / last.total_processed) * 100, 1) if last.total_processed else 0.0

        delivered_cum: list[dict[str, float | int]] = []
        running_delivered = 0
        for bucket in w300:
            running_delivered += bucket.new_delivered
            delivered_cum.append({"ts": round(bucket.ts * 1000), "v": running_delivered})

        return {
            "source": "realtime-rollup",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "kpis": {
                "new_events_last_60s": new_events_60,
                "new_orders_last_60s": new_orders_60,
                "new_delivered_last_60s": new_delivered_60,
                "event_rate_per_s": event_rate_60,
                "throughput_per_s": throughput_60,
                "total_events": last.total_events_processed,
                "total_orders": last.total_processed,
                "orders_open": last.orders_open,
                "orders_preparing": last.orders_preparing,
                "orders_waiting_courier": last.orders_waiting_courier,
                "orders_delivering": last.orders_delivering,
                "orders_delivered_total": last.orders_delivered,
                "orders_delivered": last.orders_delivered,
                "orders_cancelled_total": last.orders_cancelled,
                "orders_cancelled": last.orders_cancelled,
                "couriers_available": last.couriers_available,
                "latency_avg_1m_ms": last.latency_avg_ms,
                "latency_last_ms": last.latency_last_ms,
                "created_per_min": last.created_per_min,
                "total_processed": last.total_processed,
                "delivery_rate_pct": delivery_rate_pct,
                "cancel_rate_pct": cancel_rate_pct,
            },
            "current_status": current_status,
            "by_status": current_status,
            "by_type": by_type,
            "events_by_hour": events_by_hour,
            "avg_time_by_status": avg_time_by_status,
            "demand_heatmap": demand_heatmap,
            "delivery_time_histogram": delivery_time_histogram,
            "series": {
                "events": _series("new_events", buckets[-60:]),
                "throughput": _series("new_orders", buckets[-60:]),
                "created": _series("created_per_min", buckets[-60:]),
                "open": _series("orders_open", buckets[-60:]),
                "preparing": _series("orders_preparing", buckets[-60:]),
                "delivering": _series("orders_delivering", buckets[-60:]),
                "delivered": _series("orders_delivered", buckets[-60:]),
                "cancelled": _series("orders_cancelled", buckets[-60:]),
                "couriers": _series("couriers_available", buckets[-60:]),
                "latency": _series("latency_avg_ms", buckets[-60:]),
                "delivered_cum": delivered_cum,
            },
            "compatibility": {
                "implemented": [
                    "volume_de_pedidos_no_tempo",
                    "tempo_medio_em_cada_estado",
                    "heatmap_demanda_por_horario_e_dia_da_semana",
                    "histograma_do_tempo_total_de_entrega",
                ],
                "blocked_by_schema": [],
            },
            "error": None,
            "window_s": len(w300),
        }


def _empty(
    *,
    current_status: list[dict[str, Any]] | None = None,
    by_type: list[dict[str, Any]] | None = None,
    events_by_hour: list[dict[str, Any]] | None = None,
    avg_time_by_status: list[dict[str, Any]] | None = None,
    demand_heatmap: list[dict[str, Any]] | None = None,
    delivery_time_histogram: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "source": "realtime-rollup",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "kpis": {
            "new_events_last_60s": 0,
            "new_orders_last_60s": 0,
            "new_delivered_last_60s": 0,
            "event_rate_per_s": 0.0,
            "throughput_per_s": 0.0,
            "total_events": 0,
            "total_orders": 0,
            "orders_open": 0,
            "orders_preparing": 0,
            "orders_waiting_courier": 0,
            "orders_delivering": 0,
            "orders_delivered_total": 0,
            "orders_delivered": 0,
            "orders_cancelled_total": 0,
            "orders_cancelled": 0,
            "couriers_available": 0,
            "latency_avg_1m_ms": 0.0,
            "latency_last_ms": 0.0,
            "created_per_min": 0,
            "total_processed": 0,
            "delivery_rate_pct": 0.0,
            "cancel_rate_pct": 0.0,
        },
        "current_status": current_status or [],
        "by_status": current_status or [],
        "by_type": by_type or [],
        "events_by_hour": events_by_hour or [],
        "avg_time_by_status": avg_time_by_status or [],
        "demand_heatmap": demand_heatmap or [],
        "delivery_time_histogram": delivery_time_histogram or [],
        "series": {
            "events": [],
            "throughput": [],
            "created": [],
            "open": [],
            "preparing": [],
            "delivering": [],
            "delivered": [],
            "cancelled": [],
            "couriers": [],
            "latency": [],
            "delivered_cum": [],
        },
        "compatibility": {
            "implemented": [
                "volume_de_pedidos_no_tempo",
                "tempo_medio_em_cada_estado",
                "heatmap_demanda_por_horario_e_dia_da_semana",
                "histograma_do_tempo_total_de_entrega",
            ],
            "blocked_by_schema": [],
        },
        "error": None,
        "window_s": 0,
    }


def _dow_label(dow_num: int) -> str:
    return {
        1: "Seg",
        2: "Ter",
        3: "Qua",
        4: "Qui",
        5: "Sex",
        6: "Sab",
        7: "Dom",
    }.get(dow_num, "N/A")


def _build_delivery_histogram(
    created_at_by_order: dict[int, float],
    delivered_at_by_order: dict[int, float],
) -> list[dict[str, int | str]]:
    counts: Counter[str] = Counter()

    for order_id, delivered_at in delivered_at_by_order.items():
        created_at = created_at_by_order.get(order_id)
        if created_at is None or delivered_at < created_at:
            continue

        delivery_minutes = (delivered_at - created_at) / 60.0
        if delivery_minutes <= 10:
            counts["00-10 min"] += 1
        elif delivery_minutes <= 20:
            counts["11-20 min"] += 1
        elif delivery_minutes <= 30:
            counts["21-30 min"] += 1
        elif delivery_minutes <= 45:
            counts["31-45 min"] += 1
        elif delivery_minutes <= 60:
            counts["46-60 min"] += 1
        else:
            counts["60+ min"] += 1

    labels = ["00-10 min", "11-20 min", "21-30 min", "31-45 min", "46-60 min", "60+ min"]
    return [{"label": label, "value": int(counts[label])} for label in labels]
