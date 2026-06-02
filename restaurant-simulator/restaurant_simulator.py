import os
import random
import threading
import time

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
PREPARING_DELAY_SECONDS = float(os.getenv("PREPARING_DELAY_SECONDS", "0.8"))
ACCEPTANCE_RATE = float(os.getenv("ACCEPTANCE_RATE", "1.0"))

app = FastAPI()
active_orders: set[int] = set()
active_lock = threading.Lock()


class RestaurantOrderRequest(BaseModel):
    order_id: int
    restaurant_id: int
    client_id: int


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
            delivery_dispatched = try_trigger_delivery_dispatch(order_id)
            time.sleep(PREPARING_DELAY_SECONDS)
            update_order_status(order_id, "READY_FOR_PICKUP")

        if not delivery_dispatched:
            try_trigger_delivery_dispatch(order_id)

    except Exception as exc:
        print(f"Restaurant simulation failed for order {order_id}: {exc}")
    finally:
        with active_lock:
            active_orders.discard(order_id)


@app.get("/")
def health():
    return {"status": "restaurant simulator running"}


@app.post("/orders")
def start_order_simulation(body: RestaurantOrderRequest):
    with active_lock:
        if body.order_id in active_orders:
            return {"message": "Order already in simulation", "order_id": body.order_id}
        active_orders.add(body.order_id)

    threading.Thread(target=simulate_restaurant, args=(body.order_id,), daemon=True).start()
    return {"message": "Restaurant simulation started", "order_id": body.order_id}
