import networkx as nx

def calcular_rota(G, origem, destino):
    return nx.shortest_path(G, origem, destino, weight="length")


def montar_rota_completa(G, entregador_node, restaurante_node, cliente_node):
    r1 = calcular_rota(G, entregador_node, restaurante_node)
    r2 = calcular_rota(G, restaurante_node, cliente_node)
    return r1 + r2[1:]


def rota_para_coords(G, rota):
    return [(G.nodes[n]["y"], G.nodes[n]["x"]) for n in rota]