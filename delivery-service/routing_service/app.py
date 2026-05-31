from fastapi import FastAPI

from graph import carregar_grafo
from dijkstra import montar_rota_completa, rota_para_coords
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
if str(PARENT_DIR) not in sys.path:
    sys.path.append(str(PARENT_DIR))

from graph_utils import nearest_node

app = FastAPI()
G = carregar_grafo()


@app.get("/")
def health():
    return {"status": "routing service running"}


@app.post("/rota")
def calcular_rota_api(data: dict):
    entregador = data["entregador"]
    restaurante = data["restaurante"]
    cliente = data["cliente"]

    ent_node = nearest_node(G, entregador["lon"], entregador["lat"])
    rest_node = nearest_node(G, restaurante["lon"], restaurante["lat"])
    cli_node = nearest_node(G, cliente["lon"], cliente["lat"])

    route_to_pickup_nodes = montar_rota_completa(G, ent_node, rest_node, rest_node)
    route_to_delivery_nodes = montar_rota_completa(G, rest_node, cliente_node := cli_node, cliente_node)

    route_to_pickup = rota_para_coords(G, route_to_pickup_nodes)
    route_to_delivery = rota_para_coords(G, route_to_delivery_nodes)

    return {
        "route_to_pickup": route_to_pickup[::2] or route_to_pickup,
        "route_to_delivery": route_to_delivery[::2] or route_to_delivery,
    }
