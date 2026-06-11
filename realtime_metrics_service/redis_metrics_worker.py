import os
import time

from consumer import KinesisConsumer
from metrics_state import MetricsState
from redis_snapshot_store import RedisSnapshotStore


def main():
    state = MetricsState()
    redis_store = RedisSnapshotStore()

    if not redis_store.enabled:
        raise RuntimeError("REDIS_URL is required for redis_metrics_worker")

    consumer = KinesisConsumer(state)
    consumer.start()
    print("[redis-metrics-worker] started Kinesis -> Redis pipeline", flush=True)

    interval = float(os.getenv("REDIS_WORKER_SNAPSHOT_INTERVAL_SECONDS", "1.0"))
    try:
        while True:
            snap = state.snapshot()
            snap.setdefault("meta", {})["pipeline"] = "redis-pubsub"
            snap["meta"]["redis_status"] = redis_store.status()
            snap["meta"]["redis_published_at_ms"] = int(time.time() * 1000)
            redis_store.write_snapshot("redis-metrics", snap)
            redis_store.publish_snapshot("redis-metrics", snap)
            time.sleep(interval)
    finally:
        consumer.stop()


if __name__ == "__main__":
    main()
