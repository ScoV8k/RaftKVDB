import logging
import socket
import threading
import time
import random
import json
from database import Database
from client import ClientHandler

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Node:
    def __init__(self, node_id, host, port, peers):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.state = "follower"
        self.leader = None
        self.current_term = 0
        self.voted_for = None
        self.votes_received = 0
        self.election_timeout = self.generate_election_timeout()
        self.last_heartbeat = time.time()
        self.running = True
        
        self.state_lock = threading.Lock()
        
        self.raft_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.raft_socket.bind((host, port))
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind((host, port + 100))
        self.client_socket.listen(5)
        
        self.database = Database(self)
        self.client_handler = ClientHandler(self.database)
        logging.info(f"Node {self.node_id} started at port {self.port} (Raft) and {self.port + 100} (Client)")

    def generate_election_timeout(self):
        return random.uniform(3, 6)

    def send_message(self, message, destination):
        try:
            message_json = json.dumps(message)
            self.raft_socket.sendto(message_json.encode(), destination)
        except Exception as e:
            logging.error(f"Error sending message to {destination}: {e}")

    def broadcast(self, message):
        for peer in self.peers:
            try:
                self.send_message(message, peer)
            except Exception as e:
                logging.error(f"Error broadcasting message to peer {peer}: {e}")

    def start_election(self):
        with self.state_lock:
            if self.state != "candidate":
                logging.info(f"Node {self.node_id}: Starting election!")
                self.state = "candidate"
                self.current_term += 1
                self.voted_for = self.node_id
                self.votes_received = 1  # głosuje na siebie
                
                election_message = {
                    "type": "request_vote",
                    "candidate_id": self.node_id,
                    "term": self.current_term
                }
                self.broadcast(election_message)

    def become_leader(self):
        with self.state_lock:
            if self.state == "candidate":
                self.state = "leader"
                self.leader = self.node_id
                logging.info(f"*** Node {self.node_id} became leader for term {self.current_term}! ***")
                
                leader_message = {
                    "type": "leader_announcement",
                    "leader_id": self.node_id,
                    "term": self.current_term
                }
                self.broadcast(leader_message)

    def sync_data(self):
        sync_message = {
            "type": "sync_data",
            "leader_id": self.node_id,
            "term": self.current_term,
            "data": self.database.store
        }
        self.broadcast(sync_message)

    def sync_delete(self, key):
        delete_message = {
            "type": "sync_delete",
            "leader_id": self.node_id,
            "term": self.current_term,
            "key": key
        }
        self.broadcast(delete_message)

    def send_heartbeat(self):
        while self.running:
            try:
                if self.state == "leader":
                    heartbeat = {
                        "type": "heartbeat",
                        "leader_id": self.node_id,
                        "term": self.current_term
                    }
                    self.broadcast(heartbeat)
                    logging.info(f"Node {self.node_id} (Leader): Sending heartbeat for term {self.current_term}")
            except Exception as e:
                logging.error(f"Error sending heartbeat: {e}")
            time.sleep(1)

    def check_leader(self):
        while self.running:
            try:
                if self.state != "leader" and time.time() - self.last_heartbeat > self.election_timeout:
                    logging.warning(f"Node {self.node_id}: Election timeout!")
                    self.leader = None
                    self.start_election()
            except Exception as e:
                logging.error(f"Error checking leader: {e}")
            time.sleep(0.1)

    def handle_messages(self):
        while self.running:
            try:
                data, addr = self.raft_socket.recvfrom(1024)
                message = json.loads(data.decode())

                if "term" in message and message["term"] > self.current_term:
                    self.current_term = message["term"]
                    with self.state_lock:
                        self.state = "follower"
                        self.voted_for = None
                        self.votes_received = 0

                if message["type"] == "heartbeat":
                    if message["term"] >= self.current_term:
                        self.last_heartbeat = time.time()
                        self.leader = message["leader_id"]
                        self.current_term = message["term"]
                        with self.state_lock:
                            self.state = "follower"
                            self.voted_for = None
                        logging.info(f"Node {self.node_id}: Received heartbeat from leader {self.leader}")

                elif message["type"] == "sync_data":
                    if message["term"] >= self.current_term:
                        for key, value in message["data"].items():
                            if key not in self.database.store:
                                self.database.store[key] = value
                        print(f"Node {self.node_id}: Synchronized data from leader {message['leader_id']}")

                elif message["type"] == "sync_delete":
                    if message["term"] >= self.current_term:
                        key = message["key"]
                        if key in self.database.store:
                            del self.database.store[key]
                        print(f"Node {self.node_id}: Synchronized delete for key '{key}' from leader {message['leader_id']}")

                elif message["type"] == "sync_data":
                    if message["term"] >= self.current_term:
                        for key, value in message["data"].items():
                            if key not in self.database.store:
                                self.database.store[key] = value
                        print(f"Node {self.node_id}: Synchronized data from leader {message['leader_id']}")

                elif message["type"] == "sync_delete":
                    if message["term"] >= self.current_term:
                        key = message["key"]
                        if key in self.database.store:
                            del self.database.store[key]
                        print(f"Node {self.node_id}: Synchronized delete for key '{key}' from leader {message['leader_id']}")

                elif message["type"] == "request_vote":
                    if message["term"] >= self.current_term and (self.voted_for is None or self.voted_for == message["candidate_id"]):
                        self.current_term = message["term"]
                        self.voted_for = message["candidate_id"]
                        vote_response = {
                            "type": "vote_response",
                            "voter_id": self.node_id,
                            "candidate_id": message["candidate_id"],
                            "term": self.current_term,
                            "granted": True
                        }
                        self.send_message(vote_response, addr)
                        logging.info(f"Node {self.node_id}: Voted for {message['candidate_id']}")
                    else:
                        vote_response = {
                            "type": "vote_response",
                            "voter_id": self.node_id,
                            "candidate_id": message["candidate_id"],
                            "term": self.current_term,
                            "granted": False
                        }
                        self.send_message(vote_response, addr)

                elif message["type"] == "vote_response":
                    if message["granted"] and message["term"] == self.current_term and self.state == "candidate":
                        self.votes_received += 1
                        logging.info(f"Node {self.node_id}: Received vote from {message['voter_id']} ({self.votes_received} votes)")
                        if self.votes_received > (len(self.peers) + 1) / 2:
                            self.become_leader()

                elif message["type"] == "leader_announcement":
                    if message["term"] >= self.current_term:
                        logging.info(f"Node {self.node_id}: {message['leader_id']} is leader for term {message['term']}")
                        self.leader = message["leader_id"]
                        self.current_term = message["term"]
                        with self.state_lock:
                            self.state = "follower"
                            self.voted_for = None
                        self.last_heartbeat = time.time()

            except json.JSONDecodeError as e:
                logging.error(f"Error decoding message: {e}")
            except Exception as e:
                logging.error(f"Error handling message: {e}")

    def start_client_handler(self):
        while self.running:
            try:
                conn, addr = self.client_socket.accept()
                threading.Thread(target=self.client_handler.handle_client, args=(conn, addr), daemon=True).start()
            except Exception as e:
                logging.error(f"Error handling client connection: {e}")

    def run(self):
        try:
            threading.Thread(target=self.handle_messages, daemon=True).start()
            threading.Thread(target=self.send_heartbeat, daemon=True).start()
            threading.Thread(target=self.check_leader, daemon=True).start()
            threading.Thread(target=self.start_client_handler, daemon=True).start()
        except Exception as e:
            logging.critical(f"Error starting node: {e}")
