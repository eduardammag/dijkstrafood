import osmnx as ox
import networkx as nx
import random
import time

from service.routing_service.graph import carregar_grafo
from service.delivery_service.matcher import mapear_entregadores, encontrar_entregador


def gerar_entregadores(n, base_lat, base_lon, variacao=0.01):
    return [
        {
            "id": i,
            "lat": base_lat + random.uniform(-variacao, variacao),
            "lon": base_lon + random.uniform(-variacao, variacao),
        }
        for i in range(n)
    ]


def testar_escolha_entregador(G):
    print("\n🧪 Teste 1: escolha do entregador mais próximo")

    restaurante = {"lat": -23.5505, "lon": -46.6333}

    entregadores = [
        {"id": 1, "lat": -23.5600, "lon": -46.6500},
        {"id": 2, "lat": -23.5510, "lon": -46.6340},  # mais próximo
        {"id": 3, "lat": -23.5800, "lon": -46.7000},
    ]

    rest_node = ox.distance.nearest_nodes(G, restaurante["lon"], restaurante["lat"])
    ent_nodes = mapear_entregadores(G, entregadores)

    escolhido = encontrar_entregador(G, rest_node, ent_nodes)

    print(f"👉 Escolhido: {escolhido}")


def testar_sem_entregador(G):
    print("\n🧪 Teste 2: nenhum entregador válido")

    restaurante = {"lat": -23.5505, "lon": -46.6333}

    entregadores = [
        {"id": 1, "lat": -22.0, "lon": -45.0},  # longe demais
    ]

    rest_node = ox.distance.nearest_nodes(G, restaurante["lon"], restaurante["lat"])
    ent_nodes = mapear_entregadores(G, entregadores)

    escolhido = encontrar_entregador(G, rest_node, ent_nodes)

if escolhido is None:
    print("❌ Erro: deveria encontrar alguém")
else:
    print(f"✅ Correto: encontrou entregador mesmo distante ({escolhido})")


def testar_rota(G):
    print("\n🧪 Teste 3: cálculo de rota completa")

    restaurante = {"lat": -23.5505, "lon": -46.6333}
    cliente = {"lat": -23.5614, "lon": -46.6559}

    entregadores = [
        {"id": 1, "lat": -23.5510, "lon": -46.6320},
    ]

    rest_node = ox.distance.nearest_nodes(G, restaurante["lon"], restaurante["lat"])
    cli_node = ox.distance.nearest_nodes(G, cliente["lon"], cliente["lat"])

    ent_nodes = mapear_entregadores(G, entregadores)
    ent_id = encontrar_entregador(G, rest_node, ent_nodes)

    ent_node = ent_nodes[ent_id]

    try:
        rota1 = nx.shortest_path(G, ent_node, rest_node, weight="length")
        rota2 = nx.shortest_path(G, rest_node, cli_node, weight="length")

        rota_total = rota1 + rota2[1:]

        print(f"✅ Rota calculada")
        print(f"📍 Nós até restaurante: {len(rota1)}")
        print(f"📍 Nós até cliente: {len(rota2)}")
        print(f"📍 Total: {len(rota_total)}")

    except Exception as e:
        print(f"❌ Erro na rota: {e}")


def testar_performance(G):
    print("\n🧪 Teste 4: performance com muitos entregadores")

    restaurante = {"lat": -23.5505, "lon": -46.6333}

    entregadores = gerar_entregadores(100, restaurante["lat"], restaurante["lon"])

    rest_node = ox.distance.nearest_nodes(G, restaurante["lon"], restaurante["lat"])
    ent_nodes = mapear_entregadores(G, entregadores)

    start = time.time()
    escolhido = encontrar_entregador(G, rest_node, ent_nodes)
    end = time.time()

    print(f"👉 Escolhido: {escolhido}")
    print(f"⏱️ Tempo: {end - start:.4f}s")


def testar_rota_valida(G):
    print("\n🧪 Teste 5: robustez (rota sempre existe?)")

    restaurante = {"lat": -23.5505, "lon": -46.6333}
    cliente = {"lat": -23.5614, "lon": -46.6559}

    rest_node = ox.distance.nearest_nodes(G, restaurante["lon"], restaurante["lat"])
    cli_node = ox.distance.nearest_nodes(G, cliente["lon"], cliente["lat"])

    try:
        rota = nx.shortest_path(G, rest_node, cli_node, weight="length")
        print("✅ Rota válida encontrada")
    except:
        print("❌ Falha ao encontrar rota")


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    print("🚀 Iniciando testes...")

    G = carregar_grafo()

    testar_escolha_entregador(G)
    testar_sem_entregador(G)
    testar_rota(G)
    testar_performance(G)
    testar_rota_valida(G)

    print("\n🎯 Todos os testes finalizados")