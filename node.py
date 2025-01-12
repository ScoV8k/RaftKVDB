import socket
import threading
import time
import random
import json

class Node:
    def __init__(self, node_id, host, port, peers):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.state = "follower"
        self.leader = None
        self.election_in_progress = False
        self.last_heartbeat = time.time()
        self.running = True
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((host, port))
        print(f"Node {self.node_id} started at port {self.port}")

    def send_message(self, message, destination):
        try:
            message_json = json.dumps(message)
            self.socket.sendto(message_json.encode(), destination)
        except Exception as e:
            print(f"Error: {e}")

    def broadcast(self, message):
        for peer in self.peers:
            self.send_message(message, peer)

    def start_election(self):
        if not self.election_in_progress:
            print(f"\nNode {self.node_id}: Starting election!")
            self.state = "candidate"
            self.election_in_progress = True
            
            election_message = {
                "type": "election_start",
                "candidate_id": self.node_id
            }
            self.broadcast(election_message)
        
            time.sleep(2) 
            
            if self.state == "candidate":
                self.become_leader()

    def become_leader(self):
        self.state = "leader"
        self.leader = self.node_id
        print(f"\n*** Node {self.node_id} became leader! ***")
        
        leader_message = {
            "type": "leader_announcement",
            "leader_id": self.node_id
        }
        self.broadcast(leader_message)

    def send_heartbeat(self):
        while self.running:
            if self.state == "leader":
                heartbeat = {
                    "type": "heartbeat",
                    "leader_id": self.node_id
                }
                self.broadcast(heartbeat)
                print(f"Node {self.node_id} (Leader): Sending heartbeat")
            time.sleep(1)

    def check_leader(self):
        while self.running:
            if self.state == "follower":
                # jesli nie ma przez 3 sek
                if time.time() - self.last_heartbeat > 3:
                    print(f"Node {self.node_id}: No heartbeat detected!")
                    self.leader = None
                    self.start_election()
            time.sleep(1)

    def handle_messages(self):
        while self.running:
            try:
                data, addr = self.socket.recvfrom(1024)
                message = json.loads(data.decode())
                
                if message["type"] == "heartbeat":
                    self.last_heartbeat = time.time()
                    self.leader = message["leader_id"]
                    self.state = "follower"
                    self.election_in_progress = False
                    print(f"Node {self.node_id}: Received heartbeat from leader {self.leader}")

                elif message["type"] == "election_start":
                    if not self.election_in_progress:
                        print(f"Node {self.node_id}: Received election start from {message['candidate_id']}")
                        self.election_in_progress = True
                        self.state = "follower"  # blokada zeby nie byl kandydatem

                elif message["type"] == "leader_announcement":
                    print(f"Node {self.node_id}: {message['leader_id']} is leader")
                    self.leader = message["leader_id"]
                    self.state = "follower"
                    self.election_in_progress = False
                    self.last_heartbeat = time.time()

            except Exception as e:
                print(f"Error {e}")

    def run(self):
        threading.Thread(target=self.handle_messages).start()
        threading.Thread(target=self.send_heartbeat).start()
        threading.Thread(target=self.check_leader).start()

def create_network():
    nodes = []
    ports = [5000, 5001, 5002]
    
    for i, port in enumerate(ports):
        peer_ports = ports[:i] + ports[i+1:]
        peers = [("localhost", p) for p in peer_ports]
        node = Node(f"Node_{i+1}", "localhost", port, peers)
        nodes.append(node)
    
    return nodes

if __name__ == "__main__":
    print("Starting network...")
    nodes = create_network()
    
    for node in nodes:
        node.run()
    
    while True:
        time.sleep(1)