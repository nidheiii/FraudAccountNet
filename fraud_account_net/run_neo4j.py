from graph.neo4j_loader import Neo4jConnection
from graph.graph_builder import (
    build_transaction_graph,
    add_location_nodes,
    add_device_nodes
)

conn = Neo4jConnection(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="nidhi@1127"
)

build_transaction_graph("outputs/transactions_with_all_patterns.csv", conn)
add_location_nodes("outputs/transactions_with_all_patterns.csv", conn)
add_device_nodes("outputs/transactions_with_all_patterns.csv", conn)

conn.close()
print("Neo4j graph built with transactions, locations, and devices")
