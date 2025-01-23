import pytest
import socket
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from node import Node
from main import create_network, start_network, stop_network

@pytest.fixture(scope="module")
def basic_network():
    nodes = create_network()
    start_network(nodes)
    time.sleep(5)
    yield nodes
    stop_network(nodes)

@pytest.fixture(scope="function")
def client_connection(basic_network):
    leader = next(node for node in basic_network if node.state == "leader")
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(("localhost", leader.port + 100))
    client_socket.recv(1024)
    if leader.state == "leader":
        client_socket.recv(1024)
    yield client_socket, leader
    client_socket.close()

def test_leader_election(basic_network):
    leaders = [node for node in basic_network if node.state == "leader"]
    assert len(leaders) == 1
    followers = [node for node in basic_network if node.state == "follower"]
    assert len(followers) == len(basic_network) - 1

def test_leader_crud_operations(client_connection):
    sock, _ = client_connection
    
    sock.sendall(b"PUT test_key test_value\n")
    response = sock.recv(1024).decode()
    assert "SUCCESS" in response
    time.sleep(0.1)
    
    sock.sendall(b"GET test_key\n")
    response = sock.recv(1024).decode()
    assert "test_value" in response

def test_follower_redirect(basic_network):
    follower = next(node for node in basic_network if node.state == "follower")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", follower.port + 100))
    sock.recv(1024)
    
    sock.sendall(b"PUT test_key test_value\n")
    response = sock.recv(1024).decode()
    assert "Not the leader" in response
    sock.close()

def test_data_replication(basic_network, client_connection):
    sock, leader = client_connection
    
    sock.sendall(b"PUT repl_test repl_value\n")
    sock.recv(1024)
    time.sleep(1)
    
    for node in basic_network:
        if node != leader and node.state == "follower":
            assert "repl_value" == node.database.store.get("repl_test")

def test_crud_performance(client_connection):
    sock, _ = client_connection
    start_time = time.time()
    
    for i in range(10):
        sock.sendall(f"PUT perf_key_{i} value_{i}\n".encode())
        sock.recv(1024)
        time.sleep(0.1)
    
    duration = time.time() - start_time
    assert duration < 5.0

def test_concurrent_operations(basic_network):
    leader = next(node for node in basic_network if node.state == "leader")
    
    def run_operations():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", leader.port + 100))
        sock.recv(2048)
        
        for i in range(5):
            sock.sendall(f"PUT conc_key_{threading.get_ident()}_{i} value\n".encode())
            sock.recv(1024)
            time.sleep(0.1)
        sock.close()
    
    threads = []
    for _ in range(3):
        t = threading.Thread(target=run_operations)
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
    
    time.sleep(1)
    total_keys = sum(1 for key in leader.database.store.keys() if key.startswith("conc_key"))
    assert total_keys == 15

def test_cluster_stability(client_connection):
    sock, _ = client_connection
    
    sock.sendall(b"STATUS\n")
    time.sleep(0.1)
    response = sock.recv(1024).decode()
    assert "Database" in response