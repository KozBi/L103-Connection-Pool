import socket
import json

HOST = "127.0.0.1"  # The server hostname or IP address
PORT = 65432  # The port used by the server

class Client:
    def __init__(self):
        pass

    def handle_response(self,rspse):
        if type(rspse) == str:
            return rspse
        if type(rspse) == dict:
            for value in rspse.values(): #get values from dic
                return value

    def start_clinet(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            print("Connected to server. Type 'help' for available commands.")
            while True:
                command=input(">>>").strip()
                s.sendall(command.encode('utf-8'))
                data = s.recv(2024)
                response = json.loads(data.decode('utf-8'))
                if command != "stop":
                    print(self.handle_response(response))
                else:                    
                    print ("Server has benn closed")
                    break
                

if __name__=="__main__":
    client=Client()
    client.start_clinet()
