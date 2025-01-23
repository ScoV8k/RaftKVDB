import pytest
import socket
import time

from main import create_network, start_network, stop_network

@pytest.fixture(scope="module")
def raft_cluster():
    nodes = create_network()
    start_network(nodes)

    time.sleep(5)

    yield nodes

    stop_network(nodes)

def get_leader_node(nodes):
    for node in nodes:
        if node.state == "leader":
            return node
    return None

def test_put_and_get(raft_cluster):
    nodes = raft_cluster

    leader_node = get_leader_node(nodes)
    assert leader_node is not None, "Nie udało się znaleźć lidera!"

    leader_host = leader_node.host
    leader_port = leader_node.port + 100
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((leader_host, leader_port))
        banner = s.recv(1024).decode()
        assert "Welcome" in banner

        s.sendall(b"PUT key1 val1\n")
        resp_put = s.recv(1024).decode()
        assert "SUCCESS" in resp_put

        s.sendall(b"GET key1\n")
        resp_get = s.recv(1024).decode()
        assert "key1 -> val1" in resp_get


def test_get_not_existing_key(raft_cluster):
    nodes = raft_cluster

    leader_node = get_leader_node(nodes)
    assert leader_node is not None, "Nie udało się znaleźć lidera!"

    leader_host = leader_node.host
    leader_port = leader_node.port + 100
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((leader_host, leader_port))
        banner = s.recv(1024).decode()
        assert "Welcome" in banner

        s.sendall(b"GET doesntExist\n")
        resp_get = s.recv(1024).decode()
        assert "ERROR: Key not found." in resp_get

def test_put_existing_value(raft_cluster):
    nodes = raft_cluster

    leader_node = get_leader_node(nodes)
    assert leader_node is not None, "Nie udało się znaleźć lidera!"

    leader_host = leader_node.host
    leader_port = leader_node.port + 100
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((leader_host, leader_port))
        banner = s.recv(1024).decode()
        assert "Welcome" in banner

        s.sendall(b"PUT abc value1\n")
        resp_put = s.recv(1024).decode()
        assert "SUCCESS" in resp_put

        s.sendall(b"PUT abc value2\n")
        resp_put = s.recv(1024).decode()
        assert "ERROR: Key already exists." in resp_put

def test_update(raft_cluster):
    nodes = raft_cluster
    leader_node = get_leader_node(nodes)
    assert leader_node is not None, "Nie udało się znaleźć lidera!"

    leader_host = leader_node.host
    leader_port = leader_node.port + 100

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((leader_host, leader_port))

        s.sendall(b"PUT key2 val2\n")
        s.recv(1024)

        s.sendall(b"UPDATE key2 val2_updated\n")
        resp_upd = s.recv(1024).decode()
        assert "SUCCESS" in resp_upd

        s.sendall(b"GET key2\n")
        resp_get = s.recv(1024).decode()
        assert "key2 updated to val2_updated" in resp_get


def test_update_not_existing_key(raft_cluster):
    nodes = raft_cluster
    leader_node = get_leader_node(nodes)
    assert leader_node is not None, "Nie udało się znaleźć lidera!"

    leader_host = leader_node.host
    leader_port = leader_node.port + 100

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((leader_host, leader_port))
        banner = s.recv(1024).decode()
        assert "Welcome" in banner

        s.sendall(b"UPDATE doesntExist val2_updated\n")
        resp_upd = s.recv(1024).decode()
        assert "ERROR: Key not found." in resp_upd


def test_put_no_value(raft_cluster):
    nodes = raft_cluster
    leader_node = get_leader_node(nodes)
    assert leader_node is not None, "Nie udało się znaleźć lidera!"

    leader_host = leader_node.host
    leader_port = leader_node.port + 100

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((leader_host, leader_port))
        banner = s.recv(1024).decode()
        assert "Welcome" in banner

        s.sendall(b"PUT doesntExist\n")
        resp_upd = s.recv(1024).decode()
        assert "ERROR: Invalid command format." in resp_upd
    

def test_delete(raft_cluster):
    nodes = raft_cluster
    leader_node = get_leader_node(nodes)
    assert leader_node is not None, "Nie udało się znaleźć lidera!"

    leader_host = leader_node.host
    leader_port = leader_node.port + 100

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((leader_host, leader_port))

        s.sendall(b"PUT key4 val4\n")
        s.recv(1024)

        s.sendall(b"DELETE key4\n")
        resp_upd = s.recv(1024).decode()
        assert "SUCCESS" in resp_upd


def test_delete_not_existing_key(raft_cluster):
    nodes = raft_cluster
    leader_node = get_leader_node(nodes)
    assert leader_node is not None, "Nie udało się znaleźć lidera!"

    leader_host = leader_node.host
    leader_port = leader_node.port + 100

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((leader_host, leader_port))
        banner = s.recv(1024).decode()
        assert "Welcome" in banner

        s.sendall(b"DELETE doesntExist\n")
        resp_upd = s.recv(1024).decode()
        assert "ERROR: Key not found." in resp_upd
