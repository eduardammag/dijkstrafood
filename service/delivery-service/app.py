from fastapi import FastAPI
import threading
import osmnx as ox
import requests

from matcher import mapear_entregadores, encontrar_entregador
from tracking import simular_movimento
from routing.graph import carregar_grafo
from utils import gerar_rota_simples, filtrar_entregadores

app = FastAPI()
G = carregar_grafo()


@app.post("/alocar")
def alocar_entrega(data: dict):

    restaurante = data["restaurante"]
    cliente = data["cliente"]
    entregadores_geral = data["entregadores"]
    entregadores_filtrados = filtrar_entregadores(restaurante, entregadores_geral)

    # fallback: se filtro removeu todos, usa lista original
    if entregadores_filtrados:
        entregadores = entregadores_filtrados
    else:
        entregadores = entregadores_geral

    rest_node = ox.distance.nearest_nodes(G, restaurante["lon"], restaurante["lat"])

    entregador_nodes = mapear_entregadores(G, entregadores)

    entregador_id = encontrar_entregador(G, rest_node, entregador_nodes)

    if not entregador_id:
        return {"erro": "sem entregador"}

    entregador = next(e for e in entregadores if e["id"] == entregador_id)

    # chama routing-service
    try:
        response = requests.post(
            "http://routing-service/rota",
            json={
                "entregador": entregador,
                "restaurante": restaurante,
                "cliente": cliente
            },
            timeout=2
        )

        response.raise_for_status()
        rota = response.json()["rota"]

    except requests.exceptions.RequestException:
        # fallback simples se routing-service falhar
        rota = (
            gerar_rota_simples(
                (entregador["lat"], entregador["lon"]),
                (restaurante["lat"], restaurante["lon"])
            )
            +
            gerar_rota_simples(
                (restaurante["lat"], restaurante["lon"]),
                (cliente["lat"], cliente["lon"])
            )
        )

    # tracking async
    threading.Thread(
        target=simular_movimento,
        args=(entregador_id, rota),
        daemon=True,
        name=f"tracking-{entregador_id}"
    ).start()

    

    return {"entregador_id": entregador_id}