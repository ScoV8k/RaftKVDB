import socket
import time
import pytest
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


def clear_welcome_messages(sock, leader):
    sock.recv(1024)  # Welcome message
    if leader.state == "leader":
        sock.recv(1024) 

def test_node_data_replication_after_add(basic_network):
    try:
        leaders = [node for node in basic_network if node.state == "leader"]
        leader = leaders[0]
    except TimeoutError as e:
        pytest.fail(f"Test failed due to leader election timeout: {str(e)}")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", leader.port + 100))
    clear_welcome_messages(sock, leader)
    
    test_key = f"repl_test_{int(time.time())}"
    test_value = "test_value"
    
    try:
        sock.sendall(f"PUT {test_key} {test_value}\n".encode())
        response = sock.recv(1024).decode()
        assert "SUCCESS" in response
        time.sleep(2)
        
        for _ in range(3):
            leader.sync_data()
            time.sleep(1)
        
        for node in basic_network:
            if node != leader and node.state == "follower":
                assert node.database.store.get(test_key) == test_value, \
                    f"Node {node.node_id} did not replicate correctly"
    
    finally:
        if leader.database.store.get(test_key):
            sock.sendall(f"DELETE {test_key}\n".encode())
            sock.recv(1024)
        sock.close()