import networkx as nx
import matplotlib.pyplot as plt
import database
import math

if __name__ == '__main__':
    edges = database.get_influenced()
    nodes = database.get_nodes_with_degree()
    sizes = []
    G = nx.Graph()

    for node in nodes:
        G.add_node(node[0], degree = node[1])
        sizes.append(node[1] * node[1] / 10)
    G.add_edges_from(edges)
    
    pos=nx.spring_layout(G, k = 10/math.sqrt(len(nodes)))
    
    nx.draw_networkx(G, pos, node_size=sizes, with_labels=True, font_size='2', arrowsize=0, width=.01)
    nx.write_gexf(G, "test.gexf")
    plt.show()
