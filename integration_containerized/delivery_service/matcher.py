import networkx as nx

from graph_utils import nearest_node


def mapear_entregadores(G, entregadores):
    return {
        e["id"]: nearest_node(G, e["lon"], e["lat"])
        for e in entregadores
    }


def encontrar_entregador(G, restaurante_node, entregador_nodes):
    raios = [1000, 3000, 5000, 10000, None]

    for raio in raios:
        distancias = nx.single_source_dijkstra_path_length(
            G,
            restaurante_node,
            cutoff=raio,
            weight="length"
        )

        melhor = None
        menor = float("inf")

        for eid, node in entregador_nodes.items():
            if node in distancias and distancias[node] < menor:
                melhor = eid
                menor = distancias[node]

        if melhor:
            return melhor

    return None
