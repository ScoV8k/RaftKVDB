class ClientHandler:
    def __init__(self, database):
        self.database = database

    def handle_client(self, conn, addr):
        with conn:
            print(f"Client connected: {addr}")
            conn.sendall(b"Welcome to the Node database. Commands: PUT key value, GET key, UPDATE key value, DELETE key, STATUS\n")
            while True:
                try:
                    data = conn.recv(1024).decode().strip()
                    if not data:
                        break

                    command = data.split()
                    response = "ERROR: Invalid command format."

                    if command[0].upper() == "PUT" and len(command) == 3:
                        response = self.database.set(command[1], command[2])
                    elif command[0].upper() == "GET" and len(command) == 2:
                        response = self.database.get(command[1])
                    elif command[0].upper() == "UPDATE" and len(command) == 3:
                        response = self.database.update(command[1], command[2])
                    elif command[0].upper() == "DELETE" and len(command) == 2:
                        response = self.database.delete(command[1])
                    elif command[0].upper() == "STATUS" and len(command) == 1:
                        response = self.database.status()

                    conn.sendall(response.encode() + b"\n")
                except Exception as e:
                    print(f'ERROR - Error handling client connection:{e}')
                    conn.sendall(f"ERROR: {str(e)}\n".encode())
                    break
