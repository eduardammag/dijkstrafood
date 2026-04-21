import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set

import pika
import requests

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

API_URL = os.getenv("API_URL", "http://localhost:8000")
DISCOVERY_INTERVAL_SECONDS = float(os.getenv("DISCOVERY_INTERVAL_SECONDS", "10"))
MOVE_INTERVAL = float(os.getenv("MOVE_INTERVAL", "0.3"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "16"))
IGNORE_LOCATION_ERRORS = os.getenv("IGNORE_LOCATION_ERRORS", "true").lower() == "true"

executor = ThreadPoolExecutor(max_workers=MAX_INFLIGHT)
registered_queues: Set[str] = set()
registered_lock = threading.Lock()


class RecoverableProcessingError(Exception):
    pass


def create_connection(retries: int = 20, delay: int = 3):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    for attempt in range(1, retries + 1):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
            )
        except Exception as exc:
            print(f"Tentativa {attempt}/{retries} falhou ao conectar no RabbitMQ: {exc}")
            time.sleep(delay)

    raise RuntimeError("Não foi possível conectar ao RabbitMQ.")


def queue_name_for_courier(courier_id: int) -> str:
    return f"courier_orders_{courier_id}"


def fetch_courier_ids() -> List[int]:
    response = requests.get(f"{API_URL}/couriers", timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    couriers = response.json().get("couriers", [])
    return [int(item["courier_id"]) for item in couriers]


def update_order_status(order_id: int, status: str):
    response = requests.put(
        f"{API_URL}/orders/{order_id}/status",
        json={"status": status},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    print(f"Pedido {order_id} -> {status}")


def post_courier_location(courier_id: int, lat: float, lon: float, order_id):
    response = requests.post(
    f"{API_URL}/couriers/{courier_id}/location",
    json={
        "latitude": lat,
        "longitude": lon,
        "order_id": order_id
    },
    timeout=REQUEST_TIMEOUT_SECONDS,
)

    if not response.ok:
        message = f"Falha ao atualizar localização do courier {courier_id}: {response.status_code} {response.text}"
        if IGNORE_LOCATION_ERRORS:
            print(message)
            return
        raise RecoverableProcessingError(message)


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def generate_linear_route(start_lat: float, start_lon: float, end_lat: float, end_lon: float, steps: int = 8):
    route = []
    for i in range(steps + 1):
        t = i / steps
        route.append(
            {
                "lat": lerp(start_lat, end_lat, t),
                "lon": lerp(start_lon, end_lon, t),
            }
        )
    return route


def get_dispatch_data(order_id: int) -> dict:
    response = requests.get(
        f"{API_URL}/orders/{order_id}/dispatch-data",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def simulate_delivery(courier_id: int, order_id: int):
    dispatch = get_dispatch_data(order_id)

    restaurant_lat = float(dispatch["restaurant_latitude"])
    restaurant_lon = float(dispatch["restaurant_longitude"])
    client_lat = float(dispatch["client_latitude"])
    client_lon = float(dispatch["client_longitude"])

    pickup_route = generate_linear_route(
        start_lat=restaurant_lat,
        start_lon=restaurant_lon,
        end_lat=restaurant_lat,
        end_lon=restaurant_lon,
        steps=1,
    )

    delivery_route = generate_linear_route(
        start_lat=restaurant_lat,
        start_lon=restaurant_lon,
        end_lat=client_lat,
        end_lon=client_lon,
        steps=10,
    )

    update_order_status(order_id, "PICKED_UP")

    for point in pickup_route:
        post_courier_location(courier_id, point["lat"], point["lon"], order_id)
        time.sleep(MOVE_INTERVAL)

    update_order_status(order_id, "IN_TRANSIT")

    for point in delivery_route:
        post_courier_location(courier_id, point["lat"], point["lon"], order_id)
        time.sleep(MOVE_INTERVAL)

    update_order_status(order_id, "DELIVERED")


def process_message(message: dict):
    order_id = int(message["order_id"])
    courier_id = int(message["courier_id"])

    print(f"[courier-worker] Processando pedido {order_id} para courier {courier_id}")
    simulate_delivery(courier_id, order_id)


def register_queue_consumer(channel, queue_name: str):
    with registered_lock:
        if queue_name in registered_queues:
            return

        channel.queue_declare(queue=queue_name, durable=True)

        def callback(ch, method, properties, body):
            def job():
                try:
                    message = json.loads(body.decode("utf-8"))
                    process_message(message)
                    ch.connection.add_callback_threadsafe(
                        lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                    )
                except RecoverableProcessingError as exc:
                    print(f"Erro recuperável na fila {queue_name}: {exc}")
                    ch.connection.add_callback_threadsafe(
                        lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    )
                except Exception as exc:
                    print(f"Erro na fila {queue_name}: {exc}")
                    ch.connection.add_callback_threadsafe(
                        lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    )

            executor.submit(job)

        channel.basic_qos(prefetch_count=MAX_INFLIGHT)
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        registered_queues.add(queue_name)
        print(f"Consumidor registrado para {queue_name}")


def discovery_loop(channel):
    while True:
        try:
            courier_ids = fetch_courier_ids()
            for courier_id in courier_ids:
                register_queue_consumer(channel, queue_name_for_courier(courier_id))
        except Exception as exc:
            print(f"Falha ao descobrir couriers via API: {exc}")

        time.sleep(DISCOVERY_INTERVAL_SECONDS)


def main():
    connection = create_connection()
    channel = connection.channel()

    print("Courier worker genérico iniciado")

    discovery_thread = threading.Thread(target=discovery_loop, args=(channel,), daemon=True)
    discovery_thread.start()

    channel.start_consuming()


if __name__ == "__main__":
    main()