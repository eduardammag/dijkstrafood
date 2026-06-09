import threading
import time
from collections import Counter, deque

from event_parser import ParsedEvent


class MetricsState:
    def __init__(self):
        self._lock = threading.Lock()
        self._order_status: dict[int, str] = {}
        self._seen_orders: set[int] = set()
        self._order_has_courier: dict[int, bool] = {}
        self._order_courier: dict[int, int] = {}
        self._courier_available: dict[int, bool] = {}
        self._couriers_total_registered = 0
        self._couriers_available_api = 0

        self._processed_timestamps: deque[float] = deque()
        self._created_timestamps: deque[float] = deque()
        self._latency_samples: deque[tuple[float, float]] = deque()
        self._format_counts: Counter[str] = Counter()
        self._unknown_events = 0
        self._last_event_at: float | None = None
        self._last_event_processed_at: float | None = None
        self._last_event_latency_ms: float | None = None

    def apply(self, event: ParsedEvent):
        processed_now = time.time()
        event_timestamp = event.timestamp or processed_now
        latency_ms = max(0.0, (processed_now - event_timestamp) * 1000.0)

        with self._lock:
            self._last_event_at = event_timestamp
            self._last_event_processed_at = processed_now
            self._last_event_latency_ms = latency_ms
            self._processed_timestamps.append(processed_now)
            self._latency_samples.append((processed_now, latency_ms))
            self._format_counts[event.format_name] += 1
            self._prune_old(processed_now)

            if event.event_type == "UNKNOWN":
                self._unknown_events += 1
                return

            if event.event_type == "ORDER_CREATED":
                if event.order_id is not None:
                    self._seen_orders.add(event.order_id)
                    self._order_status[event.order_id] = event.status or "PENDING"
                    self._created_timestamps.append(processed_now)
                return

            if event.event_type in {"STATUS_CHANGE", "ORDER_EVENT"}:
                self._apply_status_event(event)
                return

            if event.event_type == "COURIER_AVAILABILITY":
                if event.courier_id is not None and event.courier_available is not None:
                    self._courier_available[event.courier_id] = event.courier_available
                return

            if event.event_type == "COURIER_LOCATION":
                if event.courier_id is not None and event.order_id is not None:
                    self._courier_available[event.courier_id] = False
                return

            if event.event_type == "COURIER_ASSIGNED":
                self._apply_courier_assigned(event)

    def _apply_status_event(self, event: ParsedEvent):
        if event.order_id is not None and event.status:
            self._seen_orders.add(event.order_id)
            self._order_status[event.order_id] = event.status

            if event.status == "DELIVERED":
                courier_id = self._order_courier.get(event.order_id)
                if courier_id is not None:
                    self._courier_available[courier_id] = True

        if event.event_type == "COURIER_ASSIGNED":
            self._apply_courier_assigned(event)

    def _apply_courier_assigned(self, event: ParsedEvent):
        if event.order_id is not None:
            self._seen_orders.add(event.order_id)
            self._order_has_courier[event.order_id] = True

        if event.order_id is not None and event.courier_id is not None:
            self._order_courier[event.order_id] = event.courier_id
            self._courier_available[event.courier_id] = False

    def _prune_old(self, now: float):
        cutoff = now - 60.0
        while self._processed_timestamps and self._processed_timestamps[0] < cutoff:
            self._processed_timestamps.popleft()
        while self._created_timestamps and self._created_timestamps[0] < cutoff:
            self._created_timestamps.popleft()
        while self._latency_samples and self._latency_samples[0][0] < cutoff:
            self._latency_samples.popleft()

    def update_courier_inventory(self, total_registered: int, available: int):
        with self._lock:
            self._couriers_total_registered = max(0, int(total_registered))
            self._couriers_available_api = max(0, int(available))

    def snapshot(self) -> dict:
        with self._lock:
            now = time.time()
            self._prune_old(now)

            preparing = 0
            waiting_courier = 0
            delivering = 0
            delivered = 0

            for order_id, status in self._order_status.items():
                if status == "PREPARING":
                    preparing += 1
                elif status == "READY_FOR_PICKUP":
                    if not self._order_has_courier.get(order_id, False):
                        waiting_courier += 1
                elif status in {"PICKED_UP", "IN_TRANSIT", "DELIVERING"}:
                    delivering += 1
                elif status == "DELIVERED":
                    delivered += 1

            couriers_available = sum(1 for is_available in self._courier_available.values() if is_available)
            latency_values = [sample[1] for sample in self._latency_samples]
            avg_latency_ms = (sum(latency_values) / len(latency_values)) if latency_values else 0.0

            return {
                "orders_preparing": preparing,
                "orders_waiting_courier": waiting_courier,
                "orders_delivering": delivering,
                "orders_delivered": delivered,
                "orders_completed": delivered,
                "orders_created_per_minute": len(self._created_timestamps),
                "total_orders_processed": len(self._seen_orders),
                "couriers_available": couriers_available,
                "couriers_total_registered": self._couriers_total_registered,
                "couriers_available_api": self._couriers_available_api,
                "couriers_seen_in_events": len(self._courier_available),
                "orders_processed_per_minute": len(self._processed_timestamps),
                "event_to_consumer_latency_ms_avg_1m": round(avg_latency_ms, 2),
                "event_to_consumer_latency_ms_last": round(self._last_event_latency_ms or 0.0, 2),
                "meta": {
                    "last_event_at": self._last_event_at,
                    "last_event_produced_at_ms": int(self._last_event_at * 1000) if self._last_event_at else None,
                    "last_event_processed_at_ms": int(self._last_event_processed_at * 1000) if self._last_event_processed_at else None,
                    "last_event_latency_ms": round(self._last_event_latency_ms or 0.0, 2),
                    "unknown_events": self._unknown_events,
                    "detected_event_formats": dict(self._format_counts),
                    "tracked_orders": len(self._order_status),
                    "couriers_total_registered": self._couriers_total_registered,
                    "couriers_available_api": self._couriers_available_api,
                    "couriers_seen_in_events": len(self._courier_available),
                    "tracked_couriers": len(self._courier_available),
                },
            }
