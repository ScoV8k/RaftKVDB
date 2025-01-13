import time
from node import Node

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

if __name__ == "__main__":
    print("Starting network...")
    nodes = create_network()
    start_network(nodes)

    while True:
        time.sleep(1)
