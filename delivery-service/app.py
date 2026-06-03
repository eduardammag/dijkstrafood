import os
from typing import Any, Dict, List

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from matcher import encontrar_entregador, mapear_entregadores
from utils import filtrar_entregadores, gerar_rota_simples

try:
    from routing_service.graph import carregar_grafo
    from graph_utils import nearest_node

    G = carregar_grafo()
except Exception as exc:
    print(f"Routing graph unavailable: {exc}")
    G = None
    nearest_node = None

app = FastAPI()

API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
ROUTING_URL = os.getenv("ROUTING_URL", "http://routing-service:8002/rota")
COURIER_SIMULATOR_URL = os.getenv(
    "COURIER_SIMULATOR_URL",
    "http://restaurant-simulator:8004",
).rstrip("/")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))


class DispatchRequest(BaseModel):
    order_id: int


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


def choose_courier(
    restaurant: Dict[str, Any],
    customer: Dict[str, Any],
    couriers: List[Dict[str, Any]],
) -> Dict[str, Any]:
    filtered = filtrar_entregadores(restaurant, couriers)
    eligible = filtered or couriers

    if not eligible:
        raise RuntimeError("No courier available for allocation")

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
            print(f"Graph matching failed, using geometric fallback: {exc}")

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
        print(f"Routing service failed; using straight line route: {exc}")

        route_to_restaurant = gerar_rota_simples(
            (courier["lat"], courier["lon"]),
            (restaurant["lat"], restaurant["lon"]),
        )
        route_to_client = gerar_rota_simples(
            (restaurant["lat"], restaurant["lon"]),
            (customer["lat"], customer["lon"]),
        )
        return route_to_restaurant, route_to_client


def trigger_courier_simulation(
    courier_id: int,
    order_id: int,
    route_to_pickup: list,
    route_to_delivery: list,
):
    response = requests.post(
        f"{COURIER_SIMULATOR_URL}/deliveries",
        json={
            "order_id": order_id,
            "courier_id": courier_id,
            "route_to_pickup": route_to_pickup,
            "route_to_delivery": route_to_delivery,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


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


@app.post("/dispatch")
def dispatch_delivery(body: DispatchRequest):
    try:
        order_id = int(body.order_id)
        order_data = get_order_dispatch_data(order_id)
        couriers = [
            courier
            for courier in get_available_couriers()
            if courier.get("lat") is not None and courier.get("lon") is not None
        ]

        if not couriers:
            raise HTTPException(status_code=409, detail="No courier available")

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

        allocation = alocar_entrega(
            {
                "order_id": order_id,
                "restaurante": restaurant,
                "cliente": customer,
                "entregadores": couriers,
            }
        )

        courier_id = int(allocation["entregador_id"])
        route_to_pickup = allocation["route_to_pickup"]
        route_to_delivery = allocation["route_to_delivery"]

        assign_courier_in_api(
            order_id=order_id,
            courier_id=courier_id,
            route_to_restaurant=route_to_pickup,
            route_to_client=route_to_delivery,
        )

        simulator_response = trigger_courier_simulation(
            courier_id=courier_id,
            order_id=order_id,
            route_to_pickup=route_to_pickup,
            route_to_delivery=route_to_delivery,
        )

        return {
            "message": "Delivery dispatched",
            "order_id": order_id,
            "courier_id": courier_id,
            "simulator": simulator_response,
        }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
