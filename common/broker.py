import json
import os
import time
from typing import Callable

import pika


class Broker:
    def __init__(self):
        self.host = os.getenv("BROKER_HOST", "localhost")
        self.port = int(os.getenv("BROKER_PORT", "5672"))
        self.user = os.getenv("BROKER_USER", "guest")
        self.password = os.getenv("BROKER_PASSWORD", "guest")
        self.exchange = os.getenv("BROKER_EXCHANGE", "dijkfood")

        self.connection = None
        self.channel = None

        self._connect()

    def _connect(self, retries: int = 20, delay: int = 3):
        credentials = pika.PlainCredentials(self.user, self.password)

        for attempt in range(1, retries + 1):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        credentials=credentials,
                        heartbeat=600,
                    )
                )
                self.channel = self.connection.channel()
                self.channel.exchange_declare(
                    exchange=self.exchange,
                    exchange_type="topic",
                    durable=True,
                )
                print(f"[Broker] Conectado ao RabbitMQ em {self.host}:{self.port}")
                return
            except Exception as e:
                print(f"[Broker] Tentativa {attempt}/{retries} falhou: {e}")
                time.sleep(delay)

        raise RuntimeError("Não foi possível conectar ao RabbitMQ.")

    def validate_order(self, order: dict) -> None:
        if not isinstance(order, dict):
            raise ValueError("O pedido deve ser um dicionário.")

        required_fields = ["client_id", "restaurant_id", "items"]
        for field in required_fields:
            if field not in order:
                raise ValueError(f"Campo obrigatório ausente: {field}")

        if not isinstance(order["client_id"], int):
            raise ValueError("client_id deve ser inteiro.")

        if not isinstance(order["restaurant_id"], int):
            raise ValueError("restaurant_id deve ser inteiro.")

        if not isinstance(order["items"], list) or len(order["items"]) == 0:
            raise ValueError("items deve ser uma lista não vazia.")

        for i, item in enumerate(order["items"]):
            if not isinstance(item, dict):
                raise ValueError(f"Item {i} deve ser um objeto.")

            if "name" not in item or "quantity" not in item:
                raise ValueError(f"Item {i} deve ter 'name' e 'quantity'.")

            if not isinstance(item["name"], str) or not item["name"].strip():
                raise ValueError(f"Item {i}: 'name' inválido.")

            if not isinstance(item["quantity"], int) or item["quantity"] <= 0:
                raise ValueError(f"Item {i}: 'quantity' deve ser inteiro maior que zero.")

    def get_restaurant_queue_name(self, restaurant_id: int) -> str:
        return f"restaurant.{restaurant_id}.orders"

    def get_restaurant_routing_key(self, restaurant_id: int) -> str:
        return f"restaurant.{restaurant_id}.order.created"

    def get_courier_queue_name(self, driver_id: int) -> str:
        return f"driver.{driver_id}.deliveries"

    def get_courier_routing_key(self, driver_id: int) -> str:
        return f"driver.{driver_id}.delivery.assigned"

    def publish(self, routing_key: str, message: dict):
        body = json.dumps(message).encode("utf-8")
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        print(f"[Broker] Publicado em '{routing_key}': {message}")

    def publish_order(self, order: dict):
        self.validate_order(order)

        restaurant_id = order["restaurant_id"]
        queue_name = self.get_restaurant_queue_name(restaurant_id)
        routing_key = self.get_restaurant_routing_key(restaurant_id)

        self.declare_queue_and_bind(queue_name, [routing_key])
        self.publish(routing_key, order)

    def declare_queue_and_bind(self, queue_name: str, routing_keys: list[str]):
        self.channel.queue_declare(queue=queue_name, durable=True)
        for routing_key in routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange,
                queue=queue_name,
                routing_key=routing_key,
            )
        print(f"[Broker] Fila '{queue_name}' ligada em {routing_keys}")

    def declare_restaurant_queue(self, restaurant_id: int):
        queue_name = self.get_restaurant_queue_name(restaurant_id)
        routing_key = self.get_restaurant_routing_key(restaurant_id)
        self.declare_queue_and_bind(queue_name, [routing_key])

    def declare_courier_queue(self, driver_id: int):
        queue_name = self.get_courier_queue_name(driver_id)
        routing_key = self.get_courier_routing_key(driver_id)
        self.declare_queue_and_bind(queue_name, [routing_key])

    def consume(self, queue_name: str, callback: Callable):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False,
        )
        print(f"[Broker] Consumindo fila '{queue_name}'")
        self.channel.start_consuming()

    def consume_restaurant_orders(self, restaurant_id: int, callback: Callable):
        queue_name = self.get_restaurant_queue_name(restaurant_id)
        self.declare_restaurant_queue(restaurant_id)
        self.consume(queue_name, callback)

    def consume_courier_deliveries(self, driver_id: int, callback: Callable):
        queue_name = self.get_courier_queue_name(driver_id)
        self.declare_courier_queue(driver_id)
        self.consume(queue_name, callback)

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass