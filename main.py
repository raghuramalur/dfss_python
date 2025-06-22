import threading
import time
from node import Node
from config import CLUSTER_CONFIG # Import from the new config file

if __name__ == "__main__":
    print("--- Final Version: Launching a 5-Node Fault-Tolerant Cluster ---")
    nodes = {}
    for node_id, node_info in CLUSTER_CONFIG.items():
        # Create config for peers (all other nodes)
        peers_config = {p_id: p_info for p_id, p_info in CLUSTER_CONFIG.items() if p_id != node_id}
        node = Node(node_id, node_info["host"], node_info["port"], peers_config)
        nodes[node_id] = node
        thread = threading.Thread(target=node.start, daemon=True)
        thread.start()
    
    print("\n--- Cluster is running. Use usage.py or stress_test.py in another terminal. ---")
    print("--- To demonstrate fault tolerance, press Ctrl+C here to stop a node. ---")
    
    current_nodes = list(nodes.values())
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n--- Shutting down a node for demonstration... ---")
        if current_nodes:
            node_to_stop = current_nodes.pop(0)
            node_to_stop.stop()
            print(f"\nStopped {node_to_stop.node_id}. The cluster will elect a new leader.")
            # Keep the rest of the nodes running
            while True:
                time.sleep(1)