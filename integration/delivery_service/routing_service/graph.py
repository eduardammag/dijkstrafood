import osmnx as ox

def carregar_grafo(path="sp.graphml"):
    G = ox.load_graphml(path)
    return G.to_undirected()