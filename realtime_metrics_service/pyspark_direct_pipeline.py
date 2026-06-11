from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any

from consumer import KinesisConsumer
from event_parser import ParsedEvent
from metrics_state import MetricsState


class PySparkDirectPipeline(threading.Thread):
    def __init__(self, on_snapshot, interval_seconds: float = 1.0, window_seconds: float = 60.0):
        super().__init__(daemon=True)
        self.on_snapshot = on_snapshot
        self.interval_seconds = interval_seconds
        self.window_seconds = window_seconds
        self.state = MetricsState()
        self._events: deque[dict[str, Any]] = deque()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._consumer = KinesisConsumer(self.state, on_event=self._capture_event)
        self._spark = None
        self._spark_error: str | None = None

    def stop(self) -> None:
        self._stop_event.set()
        self._consumer.stop()
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                pass

    def run(self) -> None:
        self._start_spark()
        self._consumer.start()
        print("[pyspark-direct] started Kinesis -> PySpark -> Dashboard pipeline", flush=True)

        while not self._stop_event.is_set():
            snapshot = self.snapshot()
            self.on_snapshot(snapshot)
            time.sleep(self.interval_seconds)

    def snapshot(self) -> dict[str, Any]:
        return self._build_snapshot()

    def _start_spark(self) -> None:
        try:
            from pyspark.sql import SparkSession

            self._spark = (
                SparkSession.builder.appName("DijkFoodPySparkDirectMetrics")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("ERROR")
        except Exception as exc:
            self._spark_error = str(exc)
            self._spark = None

    def _capture_event(self, event: ParsedEvent) -> None:
        now = time.time()
        event_timestamp = event.timestamp or now
        row = {
            "processed_at": now,
            "latency_ms": max(0.0, (now - event_timestamp) * 1000.0),
            "event_type": event.event_type,
            "status": event.status,
            "format_name": event.format_name,
        }

        with self._lock:
            self._events.append(row)
            self._prune_locked(now)

    def _build_snapshot(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            self._prune_locked(now)
            rows = list(self._events)

        snapshot = self.state.snapshot()
        snapshot.setdefault("meta", {})["pipeline"] = "pyspark-direct"
        snapshot["meta"]["pyspark_enabled"] = self._spark is not None
        snapshot["meta"]["pyspark_error"] = self._spark_error
        snapshot["meta"]["pyspark_window_seconds"] = self.window_seconds
        snapshot["meta"]["pyspark_rows_in_window"] = len(rows)

        spark_summary = self._spark_summary(rows)
        snapshot["pyspark"] = spark_summary
        if spark_summary.get("latency_avg_ms") is not None:
            snapshot["event_to_consumer_latency_ms_avg_1m"] = spark_summary["latency_avg_ms"]
        if spark_summary.get("events_per_minute") is not None:
            snapshot["orders_processed_per_minute"] = spark_summary["events_per_minute"]

        return snapshot

    def _spark_summary(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if self._spark is None or not rows:
            return {
                "events_per_minute": len(rows),
                "latency_avg_ms": None,
                "by_event_type": {},
                "by_status": {},
            }

        try:
            from pyspark.sql import functions as F

            df = self._spark.createDataFrame(rows)
            latency_avg = df.agg(F.avg("latency_ms").alias("avg")).collect()[0]["avg"]
            by_event_type = {
                row["event_type"]: row["count"]
                for row in df.groupBy("event_type").count().collect()
            }
            by_status = {
                row["status"]: row["count"]
                for row in df.where(F.col("status").isNotNull()).groupBy("status").count().collect()
            }
            return {
                "events_per_minute": len(rows),
                "latency_avg_ms": round(float(latency_avg or 0.0), 2),
                "by_event_type": by_event_type,
                "by_status": by_status,
            }
        except Exception as exc:
            self._spark_error = str(exc)
            return {
                "events_per_minute": len(rows),
                "latency_avg_ms": None,
                "by_event_type": {},
                "by_status": {},
            }

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0]["processed_at"] < cutoff:
            self._events.popleft()
