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

def test_put_existing_key(client_connection):
    sock, _ = client_connection
    
    sock.sendall(b"PUT duplicate_key initial_value\n")
    sock.recv(1024)
    
    sock.sendall(b"PUT duplicate_key new_value\n")
    response = sock.recv(1024).decode()
    assert "ERROR: Key already exists" in response

def test_update_nonexistent_key(client_connection):
    sock, _ = client_connection
    sock.sendall(b"UPDATE nonexistent_key value\n")
    response = sock.recv(1024).decode()
    assert "ERROR: Key not found" in response

def test_delete_nonexistent_key(client_connection):
    sock, _ = client_connection
    sock.sendall(b"DELETE nonexistent_key\n")
    response = sock.recv(1024).decode()
    assert "ERROR: Key not found" in response

def test_multiple_updates(client_connection):
    sock, _ = client_connection
    
    sock.sendall(b"PUT multi_update_key value1\n")
    sock.recv(1024)
    
    for i in range(3):
        sock.sendall(f"UPDATE multi_update_key value{i+2}\n".encode())
        response = sock.recv(1024).decode()
        assert "SUCCESS" in response
        
        sock.sendall(b"GET multi_update_key\n")
        response = sock.recv(1024).decode()
        assert f"value{i+2}" in response

def test_delete_and_recreate(client_connection):
    sock, _ = client_connection
    
    sock.sendall(b"PUT recreate_key value1\n")
    sock.recv(1024)
    
    sock.sendall(b"DELETE recreate_key\n")
    response = sock.recv(1024).decode()
    assert "SUCCESS" in response
    
    sock.sendall(b"PUT recreate_key value2\n")
    response = sock.recv(1024).decode()
    assert "SUCCESS" in response



def test_invalid_node_operations(client_connection):
    sock, _ = client_connection
    
    # Invalid address format
    sock.sendall(b"ADD-NODE invalid_address\n")
    response = sock.recv(1024).decode()
    assert "ERROR: Invalid address format" in response
    
    # Remove non-existent node
    sock.sendall(b"REMOVE-NODE localhost:9999\n")
    response = sock.recv(1024).decode()
    assert "ERROR: Node" in response and "does not exist" in response

def test_add_node(client_connection):
    sock, leader = client_connection
    leader.match_index = {}  # Initialize match_index
    
    sock.sendall(b"ADD-NODE localhost:7003\n")
    response = sock.recv(1024).decode()
    assert "SUCCESS" in response
    time.sleep(1)

# Add this helper function
def clear_welcome_messages(sock, leader):
    sock.recv(1024)  # Welcome message
    if leader.state == "leader":
        sock.recv(1024)  # Control cluster commands
    
def test_remove_node(basic_network):
    leader = next(node for node in basic_network if node.state == "leader")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", leader.port + 100))
    clear_welcome_messages(sock, leader)
    
    try:
        if leader.peers:
            peer = leader.peers[0]
            # First initialize match_index
            if not hasattr(leader, 'match_index'):
                leader.match_index = {peer: -1 for peer in leader.peers}
            
            sock.sendall(f"REMOVE-NODE localhost:{peer[1]}\n".encode())
            response = sock.recv(1024).decode()
            assert "SUCCESS" in response
            time.sleep(2)
    finally:
        sock.close()

def test_node_data_replication_after_add(basic_network):
    leader = next(node for node in basic_network if node.state == "leader")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", leader.port + 100))
    clear_welcome_messages(sock, leader)
    
    test_key = f"repl_test_{int(time.time())}"
    test_value = "test_value"
    
    try:
        # Add data and wait for initial sync
        sock.sendall(f"PUT {test_key} {test_value}\n".encode())
        response = sock.recv(1024).decode()
        assert "SUCCESS" in response
        time.sleep(2)
        
        # Force multiple syncs
        for _ in range(3):
            leader.sync_data()
            time.sleep(1)
        
        # Verify replication on followers
        for node in basic_network:
            if node != leader and node.state == "follower":
                assert node.database.store.get(test_key) == test_value, \
                    f"Node {node.node_id} did not replicate correctly"
    
    finally:
        if leader.database.store.get(test_key):
            sock.sendall(f"DELETE {test_key}\n".encode())
            sock.recv(1024)
        sock.close()
