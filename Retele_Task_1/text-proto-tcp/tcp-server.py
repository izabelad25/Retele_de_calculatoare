import socket
import threading

HOST = "127.0.0.1"
PORT = 3333
BUFFER_SIZE = 1024

class State:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def add(self, key, value):
        with self.lock:
            self.data[key] = value
        return f"{key} added"

    def get(self, key):
        with self.lock:
            if key in self.data:
                return f"DATA {self.data[key]}"
            return "ERROR -- invalid key"

    def remove(self, key):
        with self.lock:
            if key in self.data:
                del self.data[key]
                return f"{key} removed"
            return "ERROR -- Key not found"
    
    def list_all(self):
        with self.lock:
            if not self.data:
                return "DATA -"
            pairs = [f"{k}={v}" for k,v in self.data.items()]
            return f"DATA: {','.join(pairs)}"
    
    def count(self):
        with self.lock:
            return f"DATA {len(self.data)}"
    
    def clear(self):
        with self.lock:
            self.data.clear()
            return "Data cleared"
    
    def update(self, key, value):
        with self.lock:
            if key in self.data:
                self.data[key] = value
                return "Data updated!"
            return "ERROR -- invalid key"
    
    def pop(self, key):
        with self.lock:
            if key in self.data:
                value = self.data.pop(key)
                return f"Data {value} popped"
            return "ERROR -- invalid key"
    

state = State()

def process_command(command):
    if not command:
        return "ERROR -- missing command"
    
    parts = command.split()
    cmd = parts[0].upper()

    if len(parts) < 2:
        return "Invalid command format"

    cmd, key = parts[0], parts[1]
    
    if len(parts)>=2:
        key=parts[1]

        if cmd == "GET":
            return state.get(key)
        elif cmd == "REMOVE":
            return state.remove(key)
        elif cmd== "POP":
            return state.pop(key)
    
        if len(parts) >=3:
            value = ' '.join(parts[2:])
            if cmd == "ADD":
                return state.add(key, value)
            elif cmd == "UPDATE":
                return state.update(key, value)
    
    return "ERROR -- unknown cmd / invalid arguments"

def handle_client(client_socket):
    with client_socket:
        while True:
            try:
                data = client_socket.recv(BUFFER_SIZE)
                if not data:
                    break

                command = data.decode('utf-8').strip()
                response = process_command(command)

                #gestionare inchidere aplicatie
                if response == "QUIT_SIGNAL":
                    client_socket.sendall("Connection CLOSED. \n".encode('utf-8'))
                    break
                
                response_data = f"{len(response)} {response}".encode('utf-8')
                client_socket.sendall(response_data)

            except Exception as e:
                client_socket.sendall(f"Error: {str(e)}".encode('utf-8'))
                break

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        print(f"[SERVER] Listening on {HOST}:{PORT}")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"[SERVER] Connection from {addr}")
            threading.Thread(target=handle_client, args=(client_socket,)).start()

if __name__ == "__main__":
    start_server()
