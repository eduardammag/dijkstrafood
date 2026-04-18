from pathlib import Path
from graph_utils import load_graph

CURRENT_DIR = Path(__file__).resolve().parent

def carregar_grafo(path=None):
    if path is None:
        path = CURRENT_DIR / "sp.graphml"
    return load_graph(str(path))