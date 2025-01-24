import time
import signal
import threading
import argparse
from node import Node

def create_network(ports):
    nodes = []
    for i, port in enumerate(ports):
        peer_ports = ports[:i] + ports[i + 1:]
        peers = [("127.0.0.1", p) for p in peer_ports]
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
    parser = argparse.ArgumentParser(description="Start a network of nodes.")
    parser.add_argument(
        "--ports",
        type=int,
        nargs="+",
        required=True,
        help="List of ports for the nodes (e.g., --ports 9000 9001 9002)",
    )
    args = parser.parse_args()

    ports = args.ports
    print("Starting network with ports:", ports)

    nodes = create_network(ports)
    
    def handle_exit(signum, frame):
        stop_network(nodes)
        exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    start_network(nodes)

    while True:
        time.sleep(1)
