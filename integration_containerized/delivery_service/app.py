import json
import os
import threading
import time
from typing import Any, Dict, List

import pika
import requests
from fastapi import FastAPI

from matcher import encontrar_entregador, mapear_entregadores
from utils import filtrar_entregadores, gerar_rota_simples

try:
    from routing_service.graph import carregar_grafo
    from graph_utils import nearest_node

    G = carregar_grafo()
except Exception as exc:
    print(f"Falha ao carregar grafo do routing service: {exc}")
    G = None
    nearest_node = None

app = FastAPI()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
ROUTING_URL = os.getenv("ROUTING_URL", "http://routing-service:8002/rota")

REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
DELIVERY_PREFETCH = int(os.getenv("DELIVERY_PREFETCH", "4"))
CONSUMER_RETRY_DELAY_SECONDS = float(os.getenv("CONSUMER_RETRY_DELAY_SECONDS", "5"))


def rabbit_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
    )


def courier_queue_name(courier_id: int) -> str:
    return f"courier_orders_{courier_id}"


def get_order_dispatch_data(order_id: int) -> dict:
    response = requests.get(
        f"{API_URL}/orders/{order_id}/dispatch-data",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def get_available_couriers() -> list:
    response = requests.get(
        f"{API_URL}/couriers/available",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("couriers", [])


def assign_courier_in_api(
    order_id: int,
    courier_id: int,
    route_to_restaurant: list,
    route_to_client: list,
):
    response = requests.post(
        f"{API_URL}/orders/{order_id}/assign-courier",
        json={
            "courier_id": courier_id,
            "route_to_pickup": route_to_restaurant,
            "route_to_delivery": route_to_client,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def publish_to_courier_queue(
    channel,
    order_id: int,
    courier_id: int,
    route_to_restaurant: list,
    route_to_client: list,
):
    queue_name = courier_queue_name(courier_id)

    payload = {
        "event": "delivery_assigned",
        "order_id": order_id,
        "courier_id": courier_id,
        "route_to_pickup": route_to_restaurant,
        "route_to_delivery": route_to_client,
    }

    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )


def choose_courier(
    restaurant: Dict[str, Any],
    customer: Dict[str, Any],
    couriers: List[Dict[str, Any]],
) -> Dict[str, Any]:
    filtered = filtrar_entregadores(restaurant, couriers)
    eligible = filtered or couriers

    if not eligible:
        raise RuntimeError("Nenhum entregador disponível para alocação")

    if G is not None and nearest_node is not None:
        try:
            rest_node = nearest_node(G, restaurant["lon"], restaurant["lat"])
            courier_nodes = mapear_entregadores(G, eligible)
            selected_courier_id = encontrar_entregador(G, rest_node, courier_nodes)

            if selected_courier_id:
                selected = next(
                    (c for c in eligible if int(c["id"]) == int(selected_courier_id)),
                    None,
                )
                if selected is not None:
                    return selected
        except Exception as exc:
            print(f"Falha no matching por grafo; usando fallback geométrico: {exc}")

    return eligible[0]


def calculate_routes(
    courier: Dict[str, Any],
    restaurant: Dict[str, Any],
    customer: Dict[str, Any],
) -> tuple[list, list]:
    try:
        response = requests.post(
            ROUTING_URL,
            json={
                "entregador": courier,
                "restaurante": restaurant,
                "cliente": customer,
            },
            timeout=10,
        )
        response.raise_for_status()

        payload = response.json()
        route_to_restaurant = payload["route_to_pickup"]
        route_to_client = payload["route_to_delivery"]
        return route_to_restaurant, route_to_client

    except requests.exceptions.RequestException as exc:
        print(f"Falha no routing service; usando rota simples: {exc}")

        route_to_restaurant = gerar_rota_simples(
            (courier["lat"], courier["lon"]),
            (restaurant["lat"], restaurant["lon"]),
        )
        route_to_client = gerar_rota_simples(
            (restaurant["lat"], restaurant["lon"]),
            (customer["lat"], customer["lon"]),
        )
        return route_to_restaurant, route_to_client


@app.get("/")
def health():
    return {"status": "delivery service running"}


@app.post("/alocar")
def alocar_entrega(data: dict):
    restaurant = data["restaurante"]
    customer = data["cliente"]
    couriers = data["entregadores"]

    selected_courier = choose_courier(
        restaurant=restaurant,
        customer=customer,
        couriers=couriers,
    )

    route_to_restaurant, route_to_client = calculate_routes(
        courier=selected_courier,
        restaurant=restaurant,
        customer=customer,
    )

    return {
        "order_id": data["order_id"],
        "entregador_id": int(selected_courier["id"]),
        "route_to_pickup": route_to_restaurant,
        "route_to_delivery": route_to_client,
    }


def consume_delivery_dispatch():
    queue_name = "delivery_dispatch"

    while True:
        connection = None
        try:
            connection = rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_qos(prefetch_count=DELIVERY_PREFETCH)

            def callback(ch, method, properties, body):
                try:
                    message = json.loads(body.decode("utf-8"))
                    print("Mensagem recebida em delivery_dispatch:", message)

                    if message.get("event") != "ready_for_delivery":
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    order_id = int(message["order_id"])

                    order_data = get_order_dispatch_data(order_id)
                    couriers = [
                        c
                        for c in get_available_couriers()
                        if c.get("lat") is not None and c.get("lon") is not None
                    ]

                    restaurant = {
                        "id": int(order_data["restaurant_id"]),
                        "lat": float(order_data["restaurant_latitude"]),
                        "lon": float(order_data["restaurant_longitude"]),
                    }
                    customer = {
                        "id": int(order_data["client_id"]),
                        "lat": float(order_data["client_latitude"]),
                        "lon": float(order_data["client_longitude"]),
                    }

                    if not couriers:
                        print(f"Pedido {order_id}: nenhum courier disponível")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                        return

                    alloc = alocar_entrega(
                        {
                            "order_id": order_id,
                            "restaurante": restaurant,
                            "cliente": customer,
                            "entregadores": couriers,
                        }
                    )

                    courier_id = int(alloc["entregador_id"])
                    route_to_pickup = alloc["route_to_pickup"]
                    route_to_delivery = alloc["route_to_delivery"]

                    assign_courier_in_api(
                        order_id=order_id,
                        courier_id=courier_id,
                        route_to_restaurant=route_to_pickup,
                        route_to_client=route_to_delivery,
                    )

                    publish_to_courier_queue(
                        channel=ch,
                        order_id=order_id,
                        courier_id=courier_id,
                        route_to_restaurant=route_to_pickup,
                        route_to_client=route_to_delivery,
                    )

                    print(f"Pedido {order_id} enviado para courier {courier_id}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as exc:
                    print("Erro no delivery service:", exc)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            print("Delivery service consumindo fila delivery_dispatch")
            channel.start_consuming()

        except Exception as exc:
            print("Falha no consumer delivery_dispatch:", exc)
            time.sleep(CONSUMER_RETRY_DELAY_SECONDS)

        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=consume_delivery_dispatch, daemon=True)
    thread.start()