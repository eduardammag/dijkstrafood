import os
import time

from consumer import KinesisConsumer
from courier_capacity import fetch_courier_capacity
from metrics_state import MetricsState
from redis_snapshot_store import RedisSnapshotStore


def main():
    state = MetricsState()
    redis_store = RedisSnapshotStore()
    last_courier_capacity: dict[str, int] | None = None

    if not redis_store.enabled:
        raise RuntimeError("REDIS_URL is required for redis_metrics_worker")

    consumer = KinesisConsumer(state)
    consumer.start()
    print("[redis-metrics-worker] started Kinesis -> Redis pipeline", flush=True)

    interval = float(os.getenv("REDIS_WORKER_SNAPSHOT_INTERVAL_SECONDS", "1.0"))
    try:
        while True:
            snap = state.snapshot()
            snap.setdefault("meta", {})["pipeline"] = "redis"
            snap["meta"]["redis_status"] = redis_store.status()

            courier_capacity, error = fetch_courier_capacity()
            if courier_capacity is not None:
                last_courier_capacity = courier_capacity
                snap.update(courier_capacity)
                snap["meta"]["couriers_capacity_source"] = "order-service"
                snap["meta"].pop("couriers_capacity_error", None)
            elif last_courier_capacity is not None:
                snap.update(last_courier_capacity)
                snap["meta"]["couriers_capacity_source"] = "order-service-cache"
                snap["meta"]["couriers_capacity_error"] = error
            else:
                snap["meta"]["couriers_capacity_source"] = "unavailable"
                snap["meta"]["couriers_capacity_error"] = error

            redis_store.write_snapshot("redis-metrics", snap)
            time.sleep(interval)
    finally:
        consumer.stop()


if __name__ == "__main__":
    main()
