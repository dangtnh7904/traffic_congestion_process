import osmnx as ox

# Download the road network for Hanoi, Vietnam
G = ox.graph_from_bbox((21.090747, 20.951340, 105.898247, 105.759888), network_type="drive")

# Save 
ox.save_graphml(G, "data/map/hanoi_road_network.graphml")

# Visualize
ox.plot_graph(G)