import socket
import threading
import json

class TCPTransport:
    def __init__(self, host, port, node_id):
        self.node_id = node_id; self.host = host; self.port = port
        self._server_socket = None; self._peers = {}
        self._lock = threading.Lock(); self._running = True
        self.on_peer_message = None
        self.on_client_message = None

    def start(self):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(5)
        print(f"[{self.node_id}][Transport] Listening on {self.host}:{self.port}")
        thread = threading.Thread(target=self._accept_loop, daemon=True)
        thread.start()

    def stop(self):
        self._running = False
        if self._server_socket: self._server_socket.close()
        with self._lock:
            for sock in self._peers.values(): sock.close()

    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._server_socket.accept()
                thread = threading.Thread(target=self._handle_connection, args=(conn,), daemon=True)
                thread.start()
            except OSError: break

    def dial(self, remote_node_id, remote_host, remote_port):
        if remote_node_id == self.node_id: return
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect((remote_host, remote_port))
            self._perform_handshake(conn, remote_node_id)
            thread = threading.Thread(target=self._handle_peer_connection, args=(conn, remote_node_id), daemon=True)
            thread.start()
        except Exception as e:
            print(f"[{self.node_id}][Transport] Failed to dial {remote_node_id}: {e}")

    def _perform_handshake(self, conn, expected_node_id=None):
        self._send_framed(conn, {"type": "IDENTIFY", "node_id": self.node_id})
        len_bytes = conn.recv(4)
        if not len_bytes: raise ConnectionError("Handshake failed: connection closed.")
        msg_len = int.from_bytes(len_bytes, 'big')
        data = conn.recv(msg_len)
        message = json.loads(data.decode('utf-8'))
        if message.get("type") != "IDENTIFY": raise ConnectionError("Handshake failed: expected IDENTIFY message.")
        peer_node_id = message["node_id"]
        if expected_node_id and expected_node_id != peer_node_id:
            raise ConnectionError(f"Handshake failed: connected to {peer_node_id}, expected {expected_node_id}")
        return peer_node_id

    def _handle_connection(self, conn: socket.socket):
        """First handler for all incoming connections. It routes them based on the first message."""
        try:
            len_bytes = conn.recv(4)
            if not len_bytes: return
            msg_len = int.from_bytes(len_bytes, 'big')
            data = conn.recv(msg_len)
            message = json.loads(data.decode('utf-8'))

            if message.get("type", "").startswith("CLIENT_"):
                if self.on_client_message:
                    self.on_client_message(message, conn)
            elif message.get("type") == "IDENTIFY":
                peer_node_id = message["node_id"]
                # Acknowledge the handshake
                self._send_framed(conn, {"type": "IDENTIFY", "node_id": self.node_id})
                # Promote to a full peer connection
                self._handle_peer_connection(conn, peer_node_id)
            else:
                conn.close() # Unrecognized protocol
        except (ConnectionError, ConnectionResetError, json.JSONDecodeError, OSError):
            conn.close()

    def _handle_peer_connection(self, conn: socket.socket, peer_node_id: str):
        """Handles the long-lived connection for a specific, identified peer."""
        print(f"[{self.node_id}][Transport] Connection established with {peer_node_id}.")
        with self._lock:
            if peer_node_id in self._peers: self._peers[peer_node_id].close()
            self._peers[peer_node_id] = conn
        try:
            while self._running:
                len_bytes = conn.recv(4)
                if not len_bytes: break
                msg_len = int.from_bytes(len_bytes, 'big')
                data = b'';
                while len(data) < msg_len:
                    chunk = conn.recv(msg_len - len(data))
                    if not chunk: break
                    data += chunk
                if not data or len(data) < msg_len: break
                message = json.loads(data.decode('utf-8'))
                if self.on_peer_message:
                    self.on_peer_message(message, peer_node_id)
        except (ConnectionError, ConnectionResetError, json.JSONDecodeError, OSError): pass
        finally:
            with self._lock:
                if peer_node_id in self._peers: del self._peers[peer_node_id]
            conn.close()

    def _send_framed(self, sock, message: dict):
        data = json.dumps(message).encode('utf-8')
        len_bytes = len(data).to_bytes(4, 'big')
        sock.sendall(len_bytes + data)

    def send(self, node_id, message: dict):
        with self._lock:
            peer_socket = self._peers.get(node_id)
            if peer_socket:
                try: self._send_framed(peer_socket, message)
                except: pass

    def broadcast(self, message: dict):
        with self._lock:
            for node_id, peer_socket in list(self._peers.items()):
                try: self._send_framed(peer_socket, message)
                except: pass