import time
from node import Node
import signal
import threading

def create_network():
    nodes = []
    ports = [7000, 7001, 7002]

    for i, port in enumerate(ports):
        peer_ports = ports[:i] + ports[i + 1:]
        peers = [("localhost", p) for p in peer_ports]
        node = Node(f"Node_{i+1}", "localhost", port, peers)
        nodes.append(node)

    return nodes

def start_network(nodes):
    for node in nodes:
        node.run()

def stop_network(nodes):
    print("Stopping network...")
    for node in nodes:
        node.stop()
    print("All nodes stopped.")

def start_new_node(node_id, host, port, peers):
    new_node = Node(node_id, host, port, peers)
    threading.Thread(target=new_node.run, daemon=True).start()
    return new_node

if __name__ == "__main__":
    print("Starting network...")
    nodes = create_network()
    
    def handle_exit(signum, frame):
        stop_network(nodes)
        exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    start_network(nodes)

    while True:
        time.sleep(1)