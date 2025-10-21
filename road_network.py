import osmnx as ox

# Get Hanoi road network
G = ox.graph_from_place("Hanoi, Vietnam", network_type="drive")

# Save it for later use
ox.save_graphml(G, "hanoi_road_network.graphml")

# Visualize
ox.plot_graph(G)

# Save
# ox.save_graphml(G, "hanoi_road_network.graphml")