# simulate_client.py
import socket
import time

def simulate_client_connection(host, port):
    while True:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(1)
            client_socket.connect((host, port))
            print(f"Client connected to {host}:{port}")
            client_socket.close()
            print(f"Client disconnected from {host}:{port}")
            
            time.sleep(0.1)
        
        except (socket.error, socket.timeout) as e:
            print(f"Connection error: {e}")
            time.sleep(0.1)  

if __name__ == "__main__":
    simulate_client_connection("localhost", 7100) 
