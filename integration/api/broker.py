import os
import json
import time
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")


def _create_connection(retries: int = 10, delay: int = 3):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    for attempt in range(1, retries + 1):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
        except Exception as e:
            print(f"Tentativa {attempt}/{retries} falhou ao conectar no RabbitMQ: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise RuntimeError("Não foi possível conectar ao RabbitMQ.")


def publish_message(queue_name: str, payload: dict):
    connection = _create_connection()
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()


def publish_new_order(order_id: int, client_id: int, restaurant_id: int, items: list):
    publish_message(
        queue_name=f"restaurant_orders_{restaurant_id}",
        payload={
            "event": "new_order",
            "order_id": order_id,
            "client_id": client_id,
            "restaurant_id": restaurant_id,
            "items": items
        }
    )


def publish_ready_for_delivery(order_id: int, restaurant_id: int):
    publish_message(
        queue_name="delivery_dispatch",
        payload={
            "event": "ready_for_delivery",
            "order_id": order_id,
            "restaurant_id": restaurant_id
        }
    )


def publish_delivery_assignment(
    order_id: int,
    courier_id: int,
    restaurant_id: int,
    client_id: int,
    route_to_pickup: list,
    route_to_delivery: list
):
    publish_message(
        queue_name=f"courier_orders_{courier_id}",
        payload={
            "event": "delivery_assigned",
            "order_id": order_id,
            "courier_id": courier_id,
            "restaurant_id": restaurant_id,
            "client_id": client_id,
            "route_to_pickup": route_to_pickup,
            "route_to_delivery": route_to_delivery
        }
    )