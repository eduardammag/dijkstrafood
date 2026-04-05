from fastapi import FastAPI
import osmnx as ox

from dijkstrafood.integration.delivery_service.routing_service.graph import carregar_grafo
from dijkstrafood.integration.delivery_service.routing_service.dijkstra import montar_rota_completa, rota_para_coords

app = FastAPI()
G = carregar_grafo()


@app.post("/rota")
def calcular_rota_api(data: dict):
    entregador = data["entregador"]
    restaurante = data["restaurante"]
    cliente = data["cliente"]

    ent_node = ox.distance.nearest_nodes(G, entregador["lon"], entregador["lat"])
    rest_node = ox.distance.nearest_nodes(G, restaurante["lon"], restaurante["lat"])
    cli_node = ox.distance.nearest_nodes(G, cliente["lon"], cliente["lat"])

    rota_nodes = montar_rota_completa(G, ent_node, rest_node, cli_node)
    rota_coords = rota_para_coords(G, rota_nodes)

    # reduz densidade, reduzindo a quantidade de pontos para otimizar
    rota_coords = rota_coords[::2]

    return {"rota": rota_coords}