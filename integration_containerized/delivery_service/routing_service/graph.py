from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent

if str(PARENT_DIR) not in sys.path:
    sys.path.append(str(PARENT_DIR))

from graph_utils import load_graph


def carregar_grafo(path=None):
    if path is None:
        path = PARENT_DIR / "sp.graphml"
    return load_graph(str(path))
