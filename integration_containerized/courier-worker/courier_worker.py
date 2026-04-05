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
COURIER_ID = int(os.getenv("COURIER_ID", "1"))
MOVE_INTERVAL = float(os.getenv("MOVE_INTERVAL", "1"))
IGNORE_LOCATION_ERRORS = os.getenv("IGNORE_LOCATION_ERRORS", "true").lower() == "true"

INITIAL_LAT = float(os.getenv("INITIAL_LAT", "-22.1200"))
INITIAL_LON = float(os.getenv("INITIAL_LON", "-51.3900"))


def update_order_status(order_id: int, status: str):
    response = requests.put(
        f"{API_URL}/orders/{order_id}/status",
        json={"status": status},
        timeout=15
    )
    response.raise_for_status()
    print(f"[STATUS] Pedido {order_id} -> {status}")


def send_location(courier_id: int, lat: float, lon: float, order_id: int):
    response = requests.post(
        f"{API_URL}/couriers/{courier_id}/location",
        json={
            "latitude": lat,
            "longitude": lon,
            "order_id": order_id
        },
        timeout=10
    )
    response.raise_for_status()
    print(f"[LOC] Courier {courier_id} -> ({lat}, {lon})")


def safe_send_location(courier_id: int, lat: float, lon: float, order_id: int):
    try:
        send_location(courier_id, lat, lon, order_id)
    except Exception as e:
        if IGNORE_LOCATION_ERRORS:
            print(f"[LOC] Falha ignorada ao enviar localização: {e}")
            return
        raise


def move_along_route(order_id: int, courier_id: int, route_points: list):
    for lat, lon in route_points:
        safe_send_location(courier_id, lat, lon, order_id)
        time.sleep(MOVE_INTERVAL)


def process_delivery(message: dict):
    order_id = message["order_id"]
    courier_id = int(message["courier_id"])
    route_to_restaurant = message["route_to_pickup"]
    route_to_client = message["route_to_delivery"]

    print(f"[COURIER {courier_id}] Pedido {order_id} recebido")
    print(f"[COURIER {courier_id}] Indo ao restaurante")

    move_along_route(order_id, courier_id, route_to_restaurant)
    update_order_status(order_id, "picked_up")

    print(f"[COURIER {courier_id}] Pedido {order_id} coletado")
    print(f"[COURIER {courier_id}] Indo ao cliente")

    update_order_status(order_id, "in_transit")
    move_along_route(order_id, courier_id, route_to_client)

    update_order_status(order_id, "delivered")
    print(f"[COURIER {courier_id}] Pedido {order_id} entregue")


def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode("utf-8"))
        print(f"[COURIER {COURIER_ID}] Mensagem recebida: {message}")

        event_type = message.get("event")
        if event_type != "delivery_assigned":
            print(f"[COURIER {COURIER_ID}] Evento ignorado: {event_type}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        message_courier_id = int(message.get("courier_id"))
        if message_courier_id != COURIER_ID:
            print(f"[COURIER {COURIER_ID}] Ignorando pedido de outro courier: {message_courier_id}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        process_delivery(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[COURIER {COURIER_ID}] Erro: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


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
                    blocked_connection_timeout=300
                )
            )
        except Exception as e:
            print(f"Tentativa {attempt}/{retries} falhou ao conectar no RabbitMQ: {e}")
            time.sleep(delay)

    raise RuntimeError("Não foi possível conectar ao RabbitMQ.")


def main():
    queue_name = f"courier_orders_{COURIER_ID}"
    connection = create_connection()

    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print(f"Courier worker {COURIER_ID} consumindo fila {queue_name}")

    safe_send_location(COURIER_ID, INITIAL_LAT, INITIAL_LON, 0)
    channel.start_consuming()


if __name__ == "__main__":
    main()
