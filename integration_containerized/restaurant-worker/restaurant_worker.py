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
WORKER_MODE = os.getenv("WORKER_MODE", "all").strip().lower()
RESTAURANT_ID = os.getenv("RESTAURANT_ID", "1")
DISCOVERY_INTERVAL_SECONDS = float(os.getenv("DISCOVERY_INTERVAL_SECONDS", "10"))
CONFIRMED_DELAY_SECONDS = float(os.getenv("CONFIRMED_DELAY_SECONDS", "0.4"))
PREPARING_DELAY_SECONDS = float(os.getenv("PREPARING_DELAY_SECONDS", "0.8"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "16"))

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


def queue_name_for_restaurant(restaurant_id: int) -> str:
    return f"restaurant_orders_{restaurant_id}"


def fetch_restaurant_ids() -> List[int]:
    if WORKER_MODE == "single":
        return [int(RESTAURANT_ID)]

    response = requests.get(f"{API_URL}/restaurants", timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    restaurants = response.json().get("restaurants", [])
    return [int(item["restaurant_id"]) for item in restaurants]


def get_order(order_id: int) -> dict:
    response = requests.get(f"{API_URL}/orders/{order_id}", timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def get_order_status(order_id: int) -> str:
    order_payload = get_order(order_id)
    return order_payload["order"]["order_status"]


def update_order_status(order_id: int, status: str) -> bool:
    response = requests.put(
        f"{API_URL}/orders/{order_id}/status",
        json={"status": status},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code == 409:
        current_status = get_order_status(order_id)
        if current_status == status:
            return False
        raise RecoverableProcessingError(
            f"Transição inválida para {status}; status atual: {current_status}; detalhe: {response.text}"
        )
    response.raise_for_status()
    print(f"Pedido {order_id} -> {status}")
    return True


def progress_order(order_id: int):
    current_status = get_order_status(order_id)

    if current_status == "READY_FOR_PICKUP":
        print(f"Pedido {order_id} já estava READY_FOR_PICKUP; nada a fazer")
        return

    if current_status == "PREPARING":
        time.sleep(PREPARING_DELAY_SECONDS)
        update_order_status(order_id, "READY_FOR_PICKUP")
        return

    if current_status != "CONFIRMED":
        raise RecoverableProcessingError(
            f"Pedido {order_id} em estado inesperado para worker de restaurante: {current_status}"
        )

    time.sleep(CONFIRMED_DELAY_SECONDS)
    update_order_status(order_id, "PREPARING")
    time.sleep(PREPARING_DELAY_SECONDS)
    update_order_status(order_id, "READY_FOR_PICKUP")


def process_message(message: dict):
    event_type = message.get("event")
    if event_type != "new_order":
        print("Evento ignorado:", event_type)
        return

    order_id = int(message["order_id"])
    restaurant_id = int(message["restaurant_id"])
    print(f"[restaurant-worker] Processando pedido {order_id} do restaurante {restaurant_id}")
    progress_order(order_id)


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
            restaurant_ids = fetch_restaurant_ids()
            for restaurant_id in restaurant_ids:
                register_queue_consumer(channel, queue_name_for_restaurant(restaurant_id))
        except Exception as exc:
            print(f"Falha ao descobrir restaurantes via API: {exc}")

        time.sleep(DISCOVERY_INTERVAL_SECONDS)


def main():
    connection = create_connection()
    channel = connection.channel()

    print(f"Restaurant worker iniciado em modo={WORKER_MODE}")

    discovery_thread = threading.Thread(target=discovery_loop, args=(channel,), daemon=True)
    discovery_thread.start()

    channel.start_consuming()


if __name__ == "__main__":
    main()