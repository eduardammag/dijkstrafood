import os
import threading
import time
from typing import Any

import requests
from fastapi import FastAPI
from pydantic import BaseModel

API_URL = os.getenv("API_URL", "http://api:8000").rstrip("/")
MOVE_INTERVAL = float(os.getenv("MOVE_INTERVAL", "0.3"))
PICKUP_WAIT_INTERVAL = float(os.getenv("PICKUP_WAIT_INTERVAL", "0.5"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
IGNORE_LOCATION_ERRORS = os.getenv("IGNORE_LOCATION_ERRORS", "true").lower() == "true"

app = FastAPI()
active_deliveries: set[int] = set()
active_lock = threading.Lock()


class DeliverySimulationRequest(BaseModel):
    order_id: int
    courier_id: int
    route_to_pickup: list[Any]
    route_to_delivery: list[Any]


def update_order_status(order_id: int, status: str):
    response = requests.put(
        f"{API_URL}/orders/{order_id}/status",
        json={"status": status},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    print(f"Order {order_id} -> {status}")


def get_order_status(order_id: int) -> str:
    response = requests.get(
        f"{API_URL}/orders/{order_id}/status",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json().get("order", {})
    return str(payload.get("order_status", "")).upper()


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
        with active_lock:
            active_deliveries.discard(body.order_id)


@app.get("/")
def health():
    return {"status": "courier simulator running"}


@app.post("/deliveries")
def start_delivery(body: DeliverySimulationRequest):
    with active_lock:
        if body.order_id in active_deliveries:
            return {"message": "Delivery already in simulation", "order_id": body.order_id}
        active_deliveries.add(body.order_id)

    threading.Thread(target=simulate_delivery, args=(body,), daemon=True).start()
    return {
        "message": "Courier simulation started",
        "order_id": body.order_id,
        "courier_id": body.courier_id,
    }
