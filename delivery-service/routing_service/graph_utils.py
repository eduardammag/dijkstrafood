import math
import networkx as nx


def load_graph(path: str):
    raw = nx.read_graphml(path)
    graph = nx.Graph()

    for node_id, attrs in raw.nodes(data=True):
        graph.add_node(
            str(node_id),
            x=float(attrs["x"]),
            y=float(attrs["y"]),
        )

    if raw.is_multigraph():
        edge_iter = raw.edges(keys=True, data=True)
        for u, v, _k, attrs in edge_iter:
            length = float(attrs.get("length", 1.0))
            u = str(u)
            v = str(v)
            if graph.has_edge(u, v):
                if length < graph[u][v]["length"]:
                    graph[u][v]["length"] = length
            else:
                graph.add_edge(u, v, length=length)
    else:
        for u, v, attrs in raw.edges(data=True):
            graph.add_edge(str(u), str(v), length=float(attrs.get("length", 1.0)))

    return graph


def nearest_node(graph, lon: float, lat: float):
    best_node = None
    best_distance = math.inf

    for node_id, attrs in graph.nodes(data=True):
        distance = (attrs["x"] - lon) ** 2 + (attrs["y"] - lat) ** 2
        if distance < best_distance:
            best_distance = distance
            best_node = node_id

    return best_node
