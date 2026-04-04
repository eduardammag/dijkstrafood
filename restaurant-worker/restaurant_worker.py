import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

sys.path.append("/app/common")
from broker import Broker  # noqa: E402


RESTAURANT_ID = int(os.getenv("RESTAURANT_ID", "1"))
PREP_TIME_SECONDS = int(os.getenv("PREP_TIME_SECONDS", "5"))


def now():
    return datetime.now(timezone.utc).isoformat()


broker = Broker()

QUEUE_NAME = broker.get_restaurant_queue_name(RESTAURANT_ID)
ROUTING_KEY = broker.get_restaurant_routing_key(RESTAURANT_ID)

broker.declare_queue_and_bind(
    queue_name=QUEUE_NAME,
    routing_keys=[ROUTING_KEY],
)


def publish_status(order_id: str, status: str, extra: Optional[dict] = None):
    payload = {
        "event": "order.status.updated",
        "order_id": order_id,
        "restaurant_id": RESTAURANT_ID,
        "status": status,
        "timestamp": now(),
    }

    if extra:
        payload.update(extra)

    broker.publish("order.status.updated", payload)


def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode("utf-8"))
        print(f"[Restaurant {RESTAURANT_ID}] Recebido: {message}")

        message_restaurant_id = int(message.get("restaurant_id"))
        if message_restaurant_id != RESTAURANT_ID:
            print(
                f"[Restaurant {RESTAURANT_ID}] Ignorado: pedido de outro restaurante "
                f"({message_restaurant_id})"
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        order_id = message.get("order_id")
        if not order_id:
            order_id = str(uuid.uuid4())

        items = message.get("items", [])
        if not isinstance(items, list) or len(items) == 0:
            raise ValueError("Pedido recebido sem items válidos.")

        client_id = message.get("client_id")
        pickup_location = message.get(
            "pickup_location",
            {
                "lat": -22.1200,
                "lng": -51.3900,
            },
        )

        publish_status(
            order_id,
            "PREPARING",
            extra={
                "client_id": client_id,
                "items": items,
            },
        )

        time.sleep(PREP_TIME_SECONDS)

        ready_event = {
            "event": "order.ready_for_pickup",
            "order_id": order_id,
            "client_id": client_id,
            "restaurant_id": RESTAURANT_ID,
            "items": items,
            "status": "READY_FOR_PICKUP",
            "pickup_location": pickup_location,
            "delivery_location": message.get("delivery_location"),
            "timestamp": now(),
        }

        broker.publish("order.ready_for_pickup", ready_event)
        publish_status(
            order_id,
            "READY_FOR_PICKUP",
            extra={
                "client_id": client_id,
            },
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[Restaurant {RESTAURANT_ID}] Erro: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


if __name__ == "__main__":
    print(
        f"[Restaurant {RESTAURANT_ID}] Worker iniciado | "
        f"fila={QUEUE_NAME} | routing_key={ROUTING_KEY}"
    )
    broker.consume(QUEUE_NAME, callback)