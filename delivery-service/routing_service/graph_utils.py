import pickle
from pathlib import Path

import networkx as nx
from scipy.spatial import cKDTree

GRAPH_CACHE_VERSION = 1
SPATIAL_INDEX_KEY = "spatial_index"


def _cache_path_for(path: str) -> Path:
    return Path(path).with_suffix(".pkl")


def _build_graph(raw: nx.Graph) -> nx.Graph:
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


def _attach_spatial_index(graph: nx.Graph) -> nx.Graph:
    node_ids = []
    coordinates = []

    for node_id, attrs in graph.nodes(data=True):
        node_ids.append(node_id)
        coordinates.append((attrs["x"], attrs["y"]))

    graph.graph[SPATIAL_INDEX_KEY] = {
        "tree": cKDTree(coordinates) if coordinates else None,
        "node_ids": node_ids,
    }
    return graph


def _load_cached_graph(cache_path: Path, source_path: Path):
    if not cache_path.exists() or cache_path.stat().st_mtime < source_path.stat().st_mtime:
        return None

    with cache_path.open("rb") as fh:
        payload = pickle.load(fh)

    if payload.get("version") != GRAPH_CACHE_VERSION:
        return None

    graph = payload.get("graph")
    if graph is None:
        return None

    return _attach_spatial_index(graph)


def _write_cached_graph(cache_path: Path, graph: nx.Graph):
    with cache_path.open("wb") as fh:
        pickle.dump(
            {
                "version": GRAPH_CACHE_VERSION,
                "graph": graph,
            },
            fh,
            protocol=pickle.HIGHEST_PROTOCOL,
        )


def load_graph(path: str):
    source_path = Path(path)
    cache_path = _cache_path_for(path)

    cached_graph = _load_cached_graph(cache_path, source_path)
    if cached_graph is not None:
        return cached_graph

    raw = nx.read_graphml(path)
    graph = _build_graph(raw)
    _write_cached_graph(cache_path, graph)
    return _attach_spatial_index(graph)


def nearest_node(graph, lon: float, lat: float):
    spatial_index = graph.graph.get(SPATIAL_INDEX_KEY)
    if not spatial_index or spatial_index["tree"] is None:
        raise ValueError("Graph spatial index is empty")

    _, index = spatial_index["tree"].query((lon, lat))
    return spatial_index["node_ids"][index]
