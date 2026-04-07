import os
import json
import threading
import time
import requests
import pika
from fastapi import FastAPI

from matcher import mapear_entregadores, encontrar_entregador
from routing_service.graph import carregar_grafo
from utils import gerar_rota_simples, filtrar_entregadores
from graph_utils import nearest_node
from delivery_service.tracking import simular_movimento

app = FastAPI()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

API_URL = os.getenv("API_URL", "http://localhost:8000")
ROUTING_URL = os.getenv("ROUTING_URL", "http://routing-service:8002/rota")

G = carregar_grafo()


def rabbit_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
    )


def get_order_dispatch_data(order_id: int) -> dict:
    response = requests.get(f"{API_URL}/orders/{order_id}/dispatch-data", timeout=15)
    response.raise_for_status()
    return response.json()


def get_available_couriers() -> list:
    response = requests.get(f"{API_URL}/couriers/available", timeout=15)
    response.raise_for_status()
    return response.json()["couriers"]


def assign_courier_in_api(order_id: int, courier_id: int, route_to_restaurant: list, route_to_client: list):
    response = requests.post(
        f"{API_URL}/orders/{order_id}/assign-courier",
        json={
            "courier_id": courier_id,
            "route_to_pickup": route_to_restaurant,
            "route_to_delivery": route_to_client,
        },
        timeout=15
    )
    response.raise_for_status()
    return response.json()


@app.get("/")
def health():
    return {"status": "delivery service running"}


def marcar_ocupado(courier_id):
    try:
        response = requests.post(
            f"{API_URL}/couriers/ocupar",
            json={"courier_id": courier_id},
            timeout=5
        )
        return response.status_code == 200
    except:
        return False


@app.post("/alocar")
def alocar_entrega(data: dict):
    restaurante = data["restaurante"]
    cliente = data["cliente"]
    entregadores_geral = data["entregadores"]
    entregadores_filtrados = filtrar_entregadores(restaurante, entregadores_geral)

    entregadores = entregadores_filtrados or entregadores_geral

    rest_node = nearest_node(G, restaurante["lon"], restaurante["lat"])
    entregador_nodes = mapear_entregadores(G, entregadores)

    for _ in range(3):
        entregador_id = encontrar_entregador(G, rest_node, entregador_nodes)

        if not entregador_id:
            return {"erro": "sem entregador"}

        if marcar_ocupado(entregador_id):
            break
    else:
        return {"erro": "nenhum entregador disponível após tentativas"}

    entregador = next(e for e in entregadores if e["id"] == entregador_id)

    try:
        response = requests.post(
            ROUTING_URL,
            json={
                "entregador": entregador,
                "restaurante": restaurante,
                "cliente": cliente
            },
            timeout=10
        )
        response.raise_for_status()
        route_to_restaurant = response.json()["route_to_pickup"]
        route_to_client = response.json()["route_to_delivery"]

    except requests.exceptions.RequestException:
        route_to_restaurant = gerar_rota_simples(
            (entregador["lat"], entregador["lon"]),
            (restaurante["lat"], restaurante["lon"])
        )
        route_to_client = gerar_rota_simples(
            (restaurante["lat"], restaurante["lon"]),
            (cliente["lat"], cliente["lon"])
        )

    rota_completa = route_to_restaurant + route_to_client

    threading.Thread(
        target=simular_movimento,
        args=(entregador_id, rota_completa),
        daemon=True
    ).start()

    return {
        "order_id": data["order_id"],
        "entregador_id": entregador_id,
        "route_to_pickup": route_to_restaurant,
        "route_to_delivery": route_to_client
    }


def consume_delivery_dispatch():
    queue_name = "delivery_dispatch"

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))
            print("Mensagem recebida em delivery_dispatch:", message)

            if message.get("event") != "ready_for_delivery":
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            order_id = message["order_id"]

            order_data = get_order_dispatch_data(order_id)
            couriers = get_available_couriers()

            restaurante = {
                "id": order_data["restaurant_id"],
                "lat": -22.1240,
                "lon": -51.3900
            }

            cliente = {
                "id": order_data["client_id"],
                "lat": -22.1300,
                "lon": -51.3980
            }

            alloc = alocar_entrega({
                "order_id": order_id,
                "restaurante": restaurante,
                "cliente": cliente,
                "entregadores": couriers
            })

            if "erro" in alloc:
                print(f"Pedido {order_id}: nenhum entregador disponível")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            assign_courier_in_api(
                order_id=order_id,
                courier_id=alloc["entregador_id"],
                route_to_client=alloc["route_to_delivery"],
                route_to_restaurant=alloc["route_to_pickup"]
            )

            print(f"Pedido {order_id} enviado para courier {alloc['entregador_id']}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print("Erro no delivery service:", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    while True:
        try:
            connection = rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=queue_name, on_message_callback=callback)

            print("Delivery service consumindo fila delivery_dispatch")
            channel.start_consuming()

        except Exception as e:
            print("Falha no consumer delivery_dispatch:", e)
            time.sleep(5)


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=consume_delivery_dispatch, daemon=True)
    thread.start()
