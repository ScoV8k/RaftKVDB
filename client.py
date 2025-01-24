class ClientHandler:
    def __init__(self, database, node):
        self.database = database
        self.node = node

    def handle_client(self, conn, addr):
        if not self.node.running:
            conn.close()
            return
        with conn:
            print(f"Client connected: {addr}")
            if self.node.state == "leader":
                conn.sendall(b"Control cluster commands: ADD-NODE [new node ip], REMOVE-NODE [node ip], CLUSTER-STATUS\n")
            conn.sendall(b"Welcome to the Node database. Commands: PUT key value, GET key, UPDATE key value, DELETE key, STATUS\n")
            while self.node.running:
                try:
                    data = conn.recv(1024).decode().strip()
                    if not data:
                        break

                    command = data.split()
                    response = "ERROR: Invalid command format."

                    if command[0].upper() == "ADD-NODE" \
                        and len(command) == 2 and self.node.state == "leader":
                        response = self.node.add_node(command[1])
                    elif command[0].upper() == "REMOVE-NODE" \
                        and len(command) == 2 and self.node.state == "leader":
                        response = self.node.remove_node(command[1])
                    elif command[0].upper() == "CLUSTER-STATUS" \
                        and len(command) == 1 and self.node.state == "leader":
                        response = self.node.get_cluster_status()
                    elif command[0].upper() == "PUT" and len(command) == 3:
                        response = self.node.handle_client_operation("SET", command[1], command[2])
                    elif command[0].upper() == "GET" and len(command) == 2:
                        response = self.database.get(command[1])
                    elif command[0].upper() == "UPDATE" and len(command) == 3:
                        response = self.node.handle_client_operation("UPDATE", command[1], command[2])
                    elif command[0].upper() == "DELETE" and len(command) == 2:
                        response = self.node.handle_client_operation("DELETE", command[1])
                    elif command[0].upper() == "STATUS" and len(command) == 1:
                        response = self.database.status()
                    elif command[0].upper() == "LOGS" and len(command) == 1:
                        response = self.database.show_logs()


                    conn.sendall(response.encode() + b"\n")
                except Exception as e:
                    print(f'ERROR - Error handling client connection:{e}')
                    conn.sendall(f"ERROR: {str(e)}\n".encode())
                    break
