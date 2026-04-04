import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

sys.path.append("/app/common")
from broker import Broker  # noqa: E402


DRIVER_ID = int(os.getenv("DRIVER_ID", "1"))
MOVE_INTERVAL_SECONDS = float(os.getenv("MOVE_INTERVAL_SECONDS", "1"))
INITIAL_LAT = float(os.getenv("DRIVER_INITIAL_LAT", "-22.1200"))
INITIAL_LNG = float(os.getenv("DRIVER_INITIAL_LNG", "-51.3900"))


def now():
    return datetime.now(timezone.utc).isoformat()


class CourierWorker:
    def __init__(self):
        self.driver_id = DRIVER_ID
        self.broker = Broker()

        self.queue_name = self.broker.get_courier_queue_name(self.driver_id)
        self.routing_key = self.broker.get_courier_routing_key(self.driver_id)

        self.current_location = {
            "lat": INITIAL_LAT,
            "lng": INITIAL_LNG,
        }

        self.broker.declare_queue_and_bind(
            queue_name=self.queue_name,
            routing_keys=[self.routing_key],
        )

    def publish_status(self, order_id: str, status: str, extra: Optional[dict] = None):
        payload = {
            "event": "delivery.status.updated",
            "order_id": order_id,
            "driver_id": self.driver_id,
            "status": status,
            "timestamp": now(),
        }

        if extra:
            payload.update(extra)

        self.broker.publish("delivery.status.updated", payload)

    def publish_location(self, order_id: str, extra: Optional[dict] = None):
        payload = {
            "event": "driver.location.updated",
            "driver_id": self.driver_id,
            "order_id": order_id,
            "lat": self.current_location["lat"],
            "lng": self.current_location["lng"],
            "timestamp": now(),
        }

        if extra:
            payload.update(extra)

        self.broker.publish("driver.location.updated", payload)

    def validate_location_point(self, point: dict, field_name: str):
        if not isinstance(point, dict):
            raise ValueError(f"{field_name} deve ser um objeto.")

        if "lat" not in point or "lng" not in point:
            raise ValueError(f"{field_name} deve conter 'lat' e 'lng'.")

        if not isinstance(point["lat"], (int, float)):
            raise ValueError(f"{field_name}.lat deve ser numérico.")

        if not isinstance(point["lng"], (int, float)):
            raise ValueError(f"{field_name}.lng deve ser numérico.")

    def validate_route(self, route: list, field_name: str):
        if not isinstance(route, list) or len(route) == 0:
            raise ValueError(f"{field_name} deve ser uma lista não vazia.")

        for i, point in enumerate(route):
            self.validate_location_point(point, f"{field_name}[{i}]")

    def move_along_route(self, order_id: str, route: list, status_during_route: str):
        self.publish_status(order_id, status_during_route)

        for point in route:
            self.current_location = {
                "lat": point["lat"],
                "lng": point["lng"],
            }
            self.publish_location(order_id)
            time.sleep(MOVE_INTERVAL_SECONDS)

    def callback(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))
            print(f"[Courier {self.driver_id}] Recebido: {message}")

            message_driver_id = int(message.get("driver_id"))
            if message_driver_id != self.driver_id:
                print(
                    f"[Courier {self.driver_id}] Ignorado: mensagem do entregador "
                    f"{message_driver_id}"
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            order_id = message.get("order_id")
            if not order_id:
                raise ValueError("Mensagem recebida sem order_id.")

            pickup_location = message.get("pickup_location")
            delivery_location = message.get("delivery_location")
            route_to_pickup = message.get("route_to_pickup")
            route_to_delivery = message.get("route_to_delivery")

            self.validate_location_point(pickup_location, "pickup_location")
            self.validate_location_point(delivery_location, "delivery_location")
            self.validate_route(route_to_pickup, "route_to_pickup")
            self.validate_route(route_to_delivery, "route_to_delivery")

            self.move_along_route(order_id, route_to_pickup, "GOING_TO_PICKUP")

            self.publish_status(
                order_id,
                "PICKED_UP",
                extra={
                    "restaurant_id": message.get("restaurant_id"),
                    "client_id": message.get("client_id"),
                    "pickup_location": pickup_location,
                },
            )

            self.move_along_route(order_id, route_to_delivery, "ON_THE_WAY")

            self.publish_status(
                order_id,
                "DELIVERED",
                extra={
                    "restaurant_id": message.get("restaurant_id"),
                    "client_id": message.get("client_id"),
                    "delivery_location": delivery_location,
                },
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f"[Courier {self.driver_id}] Erro: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start(self):
        print(
            f"[Courier {self.driver_id}] Worker iniciado | "
            f"fila={self.queue_name} | routing_key={self.routing_key}"
        )
        self.broker.consume(self.queue_name, self.callback)


if __name__ == "__main__":
    worker = CourierWorker()
    worker.start()