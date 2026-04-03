import osmnx as ox

def carregar_grafo(path="sp.graphml"):
    return ox.load_graphml(path)