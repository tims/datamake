import networkx

class DirectedGraph(networkx.DiGraph):
  def __init__(self, name=None):
    super(DirectedGraph, self).__init__()

  def bfs_walk_graph(self, starthere, edge_func=None, node_func=None):
    edges = list(networkx.bfs_edges(self, starthere))
    nodes = [starthere] + list(b for a,b in edges)
    if edge_func:
      for a,b in edges:
        edge_func(a,b)
    if node_func:
      for a in nodes:
        node_func(a)
    return nodes
