from fastapi import FastAPI
import threading
import osmnx as ox
import requests

from delivery_service.matcher import mapear_entregadores, encontrar_entregador
from delivery_service.tracking import simular_movimento
from routing_service.graph import carregar_grafo
from delivery_service.utils import gerar_rota_simples, filtrar_entregadores

app = FastAPI()
G = carregar_grafo()

USER_SERVICE_URL = "http://user-service"


@app.post("/alocar")
def alocar_entrega(data: dict):

    restaurante = data["restaurante"]
    cliente = data["cliente"]

    try:
        response = requests.get(
            "http://user-service/couriers/disponiveis",
            timeout=2
        )
        response.raise_for_status()
        entregadores_geral = response.json()

    except requests.exceptions.RequestException:
        return {"erro": "falha ao buscar entregadores"}

    # filtro inicial
    entregadores_filtrados = filtrar_entregadores(restaurante, entregadores_geral)
    entregadores = entregadores_filtrados if entregadores_filtrados else entregadores_geral

    rest_node = ox.distance.nearest_nodes(G, restaurante["lon"], restaurante["lat"])
    entregador_nodes = mapear_entregadores(G, entregadores)

    # 🔥 retry + lock via API
    for _ in range(3):
        entregador_id = encontrar_entregador(G, rest_node, entregador_nodes)

        if not entregador_id:
            return {"erro": "sem entregador"}

        # tenta ocupar via user-service
        try:
            resp = requests.post(
                f"{USER_SERVICE_URL}/couriers/ocupar",
                json={"courier_id": entregador_id},
                timeout=2
            )

            if resp.status_code == 200:
                break  # sucesso

        except requests.exceptions.RequestException:
            continue

    else:
        return {"erro": "nenhum entregador disponível após tentativas"}

    entregador = next(e for e in entregadores if e["id"] == entregador_id)

    # rota
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
        daemon=True
    ).start()

    return {"entregador_id": entregador_id}