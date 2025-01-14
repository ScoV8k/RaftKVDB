import time
import threading
from node import Node
import signal

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
        threading.Thread(target=node.run, daemon=True).start()

    # Найти текущего лидера через цикл узлов
    def monitor_leader():
        while True:
            leader = next((node.leader for node in nodes if node.leader), None)
            print(f"Current leader: {leader}" if leader else "No leader currently.")
            time.sleep(3)

    threading.Thread(target=monitor_leader, daemon=True).start()

def stop_network(nodes):
    print("Stopping network...")
    for node in nodes:
        node.running = False
    print("All nodes stopped.")

if __name__ == "__main__":
    print("Starting network...")
    nodes = create_network()

    def handle_exit(signum, frame):
        stop_network(nodes)
        exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    start_network(nodes)


    def simulate_leader_failure():
        time.sleep(10)
        leader_node = next((node for node in nodes if node.node_id == node.leader), None)
        if leader_node:
            print(f"Simulating failure of leader: {leader_node.node_id}")
            leader_node.running = False

    threading.Thread(target=simulate_leader_failure, daemon=True).start()

    while True:
        time.sleep(1)
