import networkx as nx

from graph_utils import nearest_node


def mapear_entregadores(G, entregadores):
    return {
        e["id"]: nearest_node(G, e["lon"], e["lat"])
        for e in entregadores
    }


def encontrar_entregador(G, restaurante_node, entregador_nodes):
    distancias = nx.single_source_dijkstra_path_length(
        G,
        restaurante_node,
        weight="length",
    )

    melhor = None
    menor = float("inf")

    for eid, node in entregador_nodes.items():
        distancia = distancias.get(node)
        if distancia is not None and distancia < menor:
            melhor = eid
            menor = distancia

    return melhor
