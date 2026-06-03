import os
import random
import threading
import time
from typing import Any

import requests
from fastapi import FastAPI
from pydantic import BaseModel

API_URL = os.getenv("API_URL", "http://api:8000").rstrip("/")
DELIVERY_SERVICE_URL = os.getenv(
    "DELIVERY_SERVICE_URL",
    "http://delivery-service:8001",
).rstrip("/")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
CONFIRMED_DELAY_SECONDS = float(os.getenv("CONFIRMED_DELAY_SECONDS", "0.4"))
PREPARING_DELAY_SECONDS = float(os.getenv("PREPARING_DELAY_SECONDS", "0.1"))
ACCEPTANCE_RATE = float(os.getenv("ACCEPTANCE_RATE", "1.0"))
MOVE_INTERVAL = float(os.getenv("MOVE_INTERVAL", "0.2"))
PICKUP_WAIT_INTERVAL = float(os.getenv("PICKUP_WAIT_INTERVAL", "0.5"))
DISPATCH_RETRY_INTERVAL_SECONDS = float(os.getenv("DISPATCH_RETRY_INTERVAL_SECONDS", "1.5"))
DISPATCH_MAX_ATTEMPTS = int(os.getenv("DISPATCH_MAX_ATTEMPTS", "180"))
IGNORE_LOCATION_ERRORS = os.getenv("IGNORE_LOCATION_ERRORS", "true").lower() == "true"

app = FastAPI()
active_orders: set[int] = set()
active_lock = threading.Lock()
active_deliveries: set[int] = set()
delivery_lock = threading.Lock()


class RestaurantOrderRequest(BaseModel):
    order_id: int
    restaurant_id: int
    client_id: int


class DeliverySimulationRequest(BaseModel):
    order_id: int
    courier_id: int
    route_to_pickup: list[Any]
    route_to_delivery: list[Any]


def get_order_status(order_id: int) -> str:
    response = requests.get(
        f"{API_URL}/orders/{order_id}/status",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json().get("order", {})
    return str(payload.get("order_status", "")).upper()


def update_order_status(order_id: int, status: str):
    response = requests.put(
        f"{API_URL}/orders/{order_id}/status",
        json={"status": status},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def post_courier_location(courier_id: int, lat: float, lon: float, order_id: int):
    response = requests.post(
        f"{API_URL}/couriers/{courier_id}/location",
        json={"latitude": lat, "longitude": lon, "order_id": order_id},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if response.ok:
        return

    message = (
        f"Failed to update courier location {courier_id}: "
        f"{response.status_code} {response.text}"
    )
    if IGNORE_LOCATION_ERRORS:
        print(message)
        return
    response.raise_for_status()


def normalize_route_point(point: Any) -> tuple[float, float]:
    if isinstance(point, dict):
        if "lat" in point and "lon" in point:
            return float(point["lat"]), float(point["lon"])
        if "latitude" in point and "longitude" in point:
            return float(point["latitude"]), float(point["longitude"])
    if isinstance(point, (list, tuple)) and len(point) >= 2:
        return float(point[0]), float(point[1])
    raise ValueError(f"Invalid route point: {point}")


def wait_until_ready_for_pickup(order_id: int) -> str:
    while True:
        current_status = get_order_status(order_id)

        if current_status in {"READY_FOR_PICKUP", "PICKED_UP", "IN_TRANSIT", "DELIVERED"}:
            return current_status
        if current_status == "REJECTED":
            raise RuntimeError(f"Order {order_id} was rejected before pickup")

        time.sleep(PICKUP_WAIT_INTERVAL)


def trigger_delivery_dispatch(order_id: int):
    response = requests.post(
        f"{DELIVERY_SERVICE_URL}/dispatch",
        json={"order_id": order_id},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def try_trigger_delivery_dispatch(order_id: int) -> bool:
    try:
        trigger_delivery_dispatch(order_id)
        return True
    except Exception as exc:
        print(f"Delivery dispatch failed for order {order_id}: {exc}")
        return False


def has_courier_assigned(order_id: int) -> bool:
    try:
        response = requests.get(
            f"{API_URL}/orders/{order_id}/dispatch-data",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("courier_id") is not None
    except Exception as exc:
        print(f"Failed to check courier assignment for order {order_id}: {exc}")
        return False


def ensure_delivery_dispatch(order_id: int) -> bool:
    for attempt in range(1, DISPATCH_MAX_ATTEMPTS + 1):
        if has_courier_assigned(order_id):
            return True

        if try_trigger_delivery_dispatch(order_id):
            return True

        current_status = get_order_status(order_id)
        if current_status in {"DELIVERED", "REJECTED"}:
            return False

        time.sleep(DISPATCH_RETRY_INTERVAL_SECONDS)

    print(f"Delivery dispatch exhausted for order {order_id}")
    return False


def simulate_restaurant(order_id: int):
    try:
        current_status = get_order_status(order_id)
        delivery_dispatched = False

        if current_status in {"DELIVERED", "REJECTED"}:
            return

        if current_status == "PENDING":
            if random.random() > ACCEPTANCE_RATE:
                update_order_status(order_id, "REJECTED")
                return
            update_order_status(order_id, "CONFIRMED")
            current_status = "CONFIRMED"

        if current_status == "CONFIRMED":
            time.sleep(CONFIRMED_DELAY_SECONDS)
            update_order_status(order_id, "PREPARING")
            current_status = "PREPARING"

        if current_status == "PREPARING":
            # Never block status progression on dispatch retries.
            # If dispatch is flaky, order still moves to READY_FOR_PICKUP.
            delivery_dispatched = try_trigger_delivery_dispatch(order_id)
            time.sleep(PREPARING_DELAY_SECONDS)
            update_order_status(order_id, "READY_FOR_PICKUP")

        if not delivery_dispatched:
            ensure_delivery_dispatch(order_id)

    except Exception as exc:
        print(f"Restaurant simulation failed for order {order_id}: {exc}")
    finally:
        with active_lock:
            active_orders.discard(order_id)


def simulate_delivery(body: DeliverySimulationRequest):
    try:
        for point in body.route_to_pickup:
            lat, lon = normalize_route_point(point)
            post_courier_location(body.courier_id, lat, lon, body.order_id)
            time.sleep(MOVE_INTERVAL)

        pickup_status = wait_until_ready_for_pickup(body.order_id)
        if pickup_status == "READY_FOR_PICKUP":
            update_order_status(body.order_id, "PICKED_UP")

        transit_status = get_order_status(body.order_id)
        if transit_status not in {"IN_TRANSIT", "DELIVERED"}:
            update_order_status(body.order_id, "IN_TRANSIT")

        for point in body.route_to_delivery:
            lat, lon = normalize_route_point(point)
            post_courier_location(body.courier_id, lat, lon, body.order_id)
            time.sleep(MOVE_INTERVAL)

        if get_order_status(body.order_id) != "DELIVERED":
            update_order_status(body.order_id, "DELIVERED")

    except Exception as exc:
        print(f"Courier simulation failed for order {body.order_id}: {exc}")
    finally:
        with delivery_lock:
            active_deliveries.discard(body.order_id)


@app.get("/")
def health():
    return {"status": "restaurant and courier simulator running"}


@app.post("/orders")
def start_order_simulation(body: RestaurantOrderRequest):
    with active_lock:
        if body.order_id in active_orders:
            return {"message": "Order already in simulation", "order_id": body.order_id}
        active_orders.add(body.order_id)

    threading.Thread(target=simulate_restaurant, args=(body.order_id,), daemon=True).start()
    return {"message": "Restaurant simulation started", "order_id": body.order_id}


@app.post("/deliveries")
def start_delivery(body: DeliverySimulationRequest):
    with delivery_lock:
        if body.order_id in active_deliveries:
            return {"message": "Delivery already in simulation", "order_id": body.order_id}
        active_deliveries.add(body.order_id)

    threading.Thread(target=simulate_delivery, args=(body,), daemon=True).start()
    return {
        "message": "Courier simulation started",
        "order_id": body.order_id,
        "courier_id": body.courier_id,
    }
