from __future__ import annotations

import json
import os
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any


class LatencyMetricsStore:
    def __init__(self) -> None:
        self.path = Path(os.getenv("LATENCY_METRICS_PATH", "latency_metrics.jsonl"))
        self.window_seconds = float(os.getenv("LATENCY_METRICS_WINDOW_SECONDS", "300"))
        self._lock = threading.Lock()
        self._samples: dict[str, deque[dict[str, Any]]] = defaultdict(deque)

    def record(self, pipeline: str, snapshot: dict[str, Any], emitted_at: float | None = None) -> dict[str, Any]:
        emitted_at = emitted_at or time.time()
        meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}

        produced_at_ms = meta.get("last_event_produced_at_ms")
        processed_at_ms = meta.get("last_event_processed_at_ms")
        recent_threshold_ms = float(os.getenv("LATENCY_RECENT_THRESHOLD_MS", "120000"))
        processed_recently = (
            isinstance(processed_at_ms, (int, float))
            and (emitted_at * 1000.0 - float(processed_at_ms)) <= recent_threshold_ms
        )

        event_to_emit_ms = self._delta_ms(emitted_at, produced_at_ms) if processed_recently else None
        consumer_to_emit_ms = self._delta_ms(emitted_at, processed_at_ms) if processed_recently else None

        sample = {
            "recorded_at": emitted_at,
            "pipeline": pipeline,
            "last_event_produced_at_ms": produced_at_ms,
            "last_event_processed_at_ms": processed_at_ms,
            "event_to_consumer_latency_ms_last": snapshot.get("event_to_consumer_latency_ms_last"),
            "event_to_consumer_latency_ms_avg_1m": snapshot.get("event_to_consumer_latency_ms_avg_1m"),
            "consumer_to_dashboard_emit_latency_ms": consumer_to_emit_ms,
            "event_to_dashboard_emit_latency_ms": event_to_emit_ms,
            "orders_processed_per_minute": snapshot.get("orders_processed_per_minute"),
            "total_orders_processed": snapshot.get("total_orders_processed"),
        }

        with self._lock:
            self._samples[pipeline].append(sample)
            self._prune_locked(emitted_at)
            self._append_jsonl(sample)

        return sample

    def summary(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            self._prune_locked(now)
            pipelines = {
                pipeline: self._summarize(list(samples))
                for pipeline, samples in self._samples.items()
            }

        return {
            "window_seconds": self.window_seconds,
            "path": str(self.path),
            "pipelines": pipelines,
        }

    def recent(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            samples = [sample for values in self._samples.values() for sample in values]
        return sorted(samples, key=lambda item: item["recorded_at"], reverse=True)[:limit]

    def _summarize(self, samples: list[dict[str, Any]]) -> dict[str, Any]:
        fields = (
            "event_to_consumer_latency_ms_last",
            "event_to_consumer_latency_ms_avg_1m",
            "consumer_to_dashboard_emit_latency_ms",
            "event_to_dashboard_emit_latency_ms",
        )
        summary: dict[str, Any] = {"samples": len(samples)}
        for field in fields:
            values = [
                float(sample[field])
                for sample in samples
                if isinstance(sample.get(field), (int, float))
            ]
            summary[field] = self._stats(values)
        return summary

    def _stats(self, values: list[float]) -> dict[str, float | None]:
        if not values:
            return {"avg": None, "p50": None, "p95": None, "last": None}

        ordered = sorted(values)
        return {
            "avg": round(sum(values) / len(values), 2),
            "p50": round(self._percentile(ordered, 50), 2),
            "p95": round(self._percentile(ordered, 95), 2),
            "last": round(values[-1], 2),
        }

    def _percentile(self, ordered: list[float], percentile: int) -> float:
        if len(ordered) == 1:
            return ordered[0]
        rank = (len(ordered) - 1) * (percentile / 100.0)
        lower = int(rank)
        upper = min(lower + 1, len(ordered) - 1)
        weight = rank - lower
        return ordered[lower] * (1.0 - weight) + ordered[upper] * weight

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self.window_seconds
        for samples in self._samples.values():
            while samples and samples[0]["recorded_at"] < cutoff:
                samples.popleft()

    def _append_jsonl(self, sample: dict[str, Any]) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as file:
                file.write(json.dumps(sample, default=str) + "\n")
        except Exception:
            pass

    def _delta_ms(self, emitted_at: float, timestamp_ms: Any) -> float | None:
        if not isinstance(timestamp_ms, (int, float)):
            return None
        return round(max(0.0, emitted_at * 1000.0 - float(timestamp_ms)), 2)
