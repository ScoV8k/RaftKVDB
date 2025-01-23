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
        
        self.database = Database()
        self.client_handler = ClientHandler(self.database, self)
        logging.info(f"Node {self.node_id} started at port {self.port} (Raft) and {self.port + 100} (Client)")

        self.next_index = {} 
        self.commit_index = -1 
        
        for peer in peers:
            self.next_index[peer] = 0


    def sync_data(self):
        """Send AppendEntries RPCs to all followers to replicate log entries."""
        if self.state != "leader":
            return

        for peer in self.peers:
            next_idx = self.next_index[peer]
            
            # Prepare entries to send
            entries = self.database.log[next_idx:] if next_idx < len(self.database.log) else []
            
            append_entries_msg = {
                "type": "append_entries",
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": next_idx - 1,
                "prev_log_term": self.database.log[next_idx - 1]["term"] if next_idx > 0 else 0,
                "entries": entries,
                "leader_commit": self.commit_index
            }
            
            self.send_message(append_entries_msg, peer)

    def generate_election_timeout(self):
        return random.uniform(3, 6)
    
    def add_node(self):
        pass

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

    # def sync_data(self):
    #     sync_message = {
    #         "type": "sync_data",
    #         "leader_id": self.node_id,
    #         "term": self.current_term,
    #         "data": self.database.store
    #     }
    #     self.broadcast(sync_message)

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
                    self.sync_data()
                    logging.info(f"Node {self.node_id} (Leader): Sending heartbeat for term {self.current_term}")
            except Exception as e:
                logging.error(f"Error sending heartbeat: {e}")
            time.sleep(1)

    def check_leader(self):
        while self.running:
            try:
                if self.state != "leader" and time.time() - self.last_heartbeat > self.election_timeout:
                    logging.warning(f"Node {self.node_id}: Election timeout! [ALARM] Election starts")
                    self.leader = None
                    self.start_election()
            except Exception as e:
                logging.error(f"Error checking leader: {e}")
            time.sleep(0.1)


    def handle_client_operation(self, operation, key, value=None):
        """Handle client operations by adding them to the log and replicating."""
        if self.state != "leader":
            return f"ERROR: Not the leader. Current leader is {self.leader}"

        log_entry = {
            "term": self.current_term,
            "operation": operation,
            "key": key,
            "value": value
        }

        log_index = self.database.append_log(log_entry)
        
        result = self.database.apply_log_entry(log_entry)

        self.commit_index = log_index
        self.database.commit_index = log_index
        self.sync_data()
        
        return result

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
                        for old_key in self.database.store:
                            if old_key not in message["data"]:
                                del self.database.store[old_key]
                        logging.info(f"Node {self.node_id}: Synchronized data from leader {self.leader}")

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
                elif message["type"] == "append_entries":
                    response = self.handle_append_entries(message, addr)
                    if response:
                        self.send_message(response, addr)

            except json.JSONDecodeError as e:
                logging.error(f"Error decoding message: {e}")
            except Exception as e:
                logging.error(f"Error handling message: {e}")

    def handle_append_entries(self, message, sender_addr):
        """Handle AppendEntries RPC from leader."""
        response = {
            "type": "append_entries_response",
            "term": self.current_term,
            "success": False,
            "node_id": self.node_id
        }
        if message["term"] < self.current_term:
            return response

        self.last_heartbeat = time.time()
        self.leader = message["leader_id"]

        if message["term"] > self.current_term:
            self.current_term = message["term"]

        prev_log_index = message["prev_log_index"]
        if prev_log_index >= len(self.database.log):
            return response
        
        if prev_log_index >= 0 and self.database.log[prev_log_index]["term"] != message["prev_log_term"]:
            return response

        for i, entry in enumerate(message["entries"]):
            log_index = prev_log_index + 1 + i
            if log_index < len(self.database.log):
                if self.database.log[log_index]["term"] != entry["term"]:
                    self.database.log = self.database.log[:log_index]
                    self.database.log.append(entry)
            else:
                self.database.log.append(entry)

        if message["leader_commit"] > self.database.commit_index:
            self.database.commit_log_entries(min(message["leader_commit"], len(self.database.log) - 1))

        response["success"] = True
        return response

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
