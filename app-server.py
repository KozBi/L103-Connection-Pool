import socket
import json
from datetime import datetime
from my_classes.CommandRouter import CommandRouter
from my_classes.DataBaseService import DataBaseService

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)
VERSION = "2.0.0"
CREATION_DATE = datetime.now()

class Server:
    def __init__(self):
        database=DataBaseService()
        self.CommandRouter=CommandRouter(VERSION,CREATION_DATE,database)
        self.response=None           

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((HOST, PORT))
            s.listen()
            print(f"Server started at {HOST}:{PORT}")
            conn, addr = s.accept()
            with conn:
                print(f"Connected by {addr} and {conn}")
                while True:
                    data = conn.recv(2024)
                    if not data:
                        break
                    command = data.decode('utf-8') # receive from client a task
                    self.response = self.CommandRouter.handle_command(command)
                    conn.sendall(json.dumps(self.response).encode('utf-8'))
                    
                    if command == "stop":
                        print("Shutting down server by user")
                        break
                    
if __name__ == "__main__":
    server=Server()
    server.start_server()

