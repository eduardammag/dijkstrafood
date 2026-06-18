import networkx as nx

from graph_utils import nearest_node


def mapear_entregadores(G, entregadores):
    return {
        e["id"]: nearest_node(G, e["lon"], e["lat"])
        for e in entregadores
    }


def encontrar_entregador(G, restaurante_node, entregador_nodes):
    raios = [1000, 3000, 5000, 10000, None]
    cutoff_inicial = max(raio for raio in raios if raio is not None)
    distancias = nx.single_source_dijkstra_path_length(
        G,
        restaurante_node,
        cutoff=cutoff_inicial,
        weight="length"
    )

    def selecionar_melhor(distancias_calculadas, limite):
        melhor = None
        menor = float("inf")

        for eid, node in entregador_nodes.items():
            distancia = distancias_calculadas.get(node)
            if distancia is None:
                continue
            if limite is not None and distancia > limite:
                continue
            if distancia < menor:
                melhor = eid
                menor = distancia

        return melhor

    for raio in raios[:-1]:
        melhor = selecionar_melhor(distancias, raio)
        if melhor:
            return melhor

    distancias_completas = nx.single_source_dijkstra_path_length(
        G,
        restaurante_node,
        weight="length"
    )
    return selecionar_melhor(distancias_completas, None)
