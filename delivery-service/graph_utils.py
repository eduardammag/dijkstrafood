import pickle
from pathlib import Path

import networkx as nx
from scipy.spatial import cKDTree


def _cache_path_for(graph_path: str) -> Path:
    source_path = Path(graph_path)
    return source_path.with_suffix(".pkl")


def _build_graph_from_graphml(path: str):
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


def _write_graph_cache(cache_path: Path, graph) -> None:
    with cache_path.open("wb") as cache_file:
        pickle.dump(graph, cache_file, protocol=pickle.HIGHEST_PROTOCOL)


def _read_graph_cache(cache_path: Path):
    with cache_path.open("rb") as cache_file:
        return pickle.load(cache_file)


def _ensure_spatial_index(graph) -> None:
    if "node_ids" in graph.graph and "node_kdtree" in graph.graph:
        return

    node_ids = []
    coordinates = []

    for node_id, attrs in graph.nodes(data=True):
        node_ids.append(node_id)
        coordinates.append((attrs["x"], attrs["y"]))

    graph.graph["node_ids"] = node_ids
    graph.graph["node_kdtree"] = cKDTree(coordinates)


def load_graph(path: str):
    source_path = Path(path)
    cache_path = _cache_path_for(path)

    use_cache = cache_path.exists() and cache_path.stat().st_mtime >= source_path.stat().st_mtime
    if use_cache:
        graph = _read_graph_cache(cache_path)
    else:
        graph = _build_graph_from_graphml(path)
        _write_graph_cache(cache_path, graph)

    _ensure_spatial_index(graph)
    return graph


def nearest_node(graph, lon: float, lat: float):
    _ensure_spatial_index(graph)
    _distance, index = graph.graph["node_kdtree"].query((lon, lat))
    return graph.graph["node_ids"][index]
