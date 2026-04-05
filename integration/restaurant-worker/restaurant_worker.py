import os
import json
import time
import requests
import pika


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

API_URL = os.getenv("API_URL", "http://localhost:8000")
RESTAURANT_ID = os.getenv("RESTAURANT_ID", "1")


def update_order_status(order_id: int, status: str):
    response = requests.put(
        f"{API_URL}/orders/{order_id}/status",
        json={"status": status},
        timeout=15
    )
    response.raise_for_status()
    print(f"Pedido {order_id} -> {status}")


def process_order(order_data: dict):
    order_id = order_data["order_id"]

    update_order_status(order_id, "confirmed")
    time.sleep(2)

    update_order_status(order_id, "preparing")
    time.sleep(5)

    update_order_status(order_id, "ready_for_delivery")


def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode("utf-8"))
        print("Mensagem recebida:", message)

        event_type = message.get("event")

        if event_type != "new_order":
            print("Evento ignorado:", event_type)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        process_order(message)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print("Erro ao processar mensagem:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    queue_name = f"restaurant_orders_{RESTAURANT_ID}"

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
    )

    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print(f"Worker do restaurante {RESTAURANT_ID} consumindo fila {queue_name}")
    channel.start_consuming()


if __name__ == "__main__":
    main()