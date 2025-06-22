import socket
import json
import random

class Client:
    """A client for interacting with the Raft key-value store cluster."""
    def __init__(self, cluster_addresses):
        self.cluster_addresses = list(cluster_addresses)
        # Start by targeting a random node
        self.leader_address = random.choice(self.cluster_addresses)

    def _send_request(self, message):
        """Sends a framed request, reads a framed response, and handles redirection."""
        # This loop will retry with different nodes if connections fail
        nodes_to_try = list(self.cluster_addresses)
        
        while nodes_to_try:
            try:
                host, port = self.leader_address
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.settimeout(3.0) # Timeout for socket operations
                conn.connect((host, port))
                
                # Send message with 4-byte length prefix
                data = json.dumps(message).encode('utf-8')
                conn.sendall(len(data).to_bytes(4, 'big') + data)
                
                # Read response with 4-byte length prefix
                len_bytes = conn.recv(4)
                if not len_bytes: return {"success": False, "error": "No response from server"}
                msg_len = int.from_bytes(len_bytes, 'big')
                
                response_data = b''
                while len(response_data) < msg_len:
                    chunk = conn.recv(msg_len - len(response_data))
                    if not chunk: raise ConnectionError("Connection lost while reading response.")
                    response_data += chunk
                
                response = json.loads(response_data.decode('utf-8'))
                conn.close()

                # If we're redirected, update the leader and retry the request
                if not response.get("success", False) and "leader" in response and response["leader"]:
                    leader_addr_str = response["leader"]
                    l_host, l_port_str = leader_addr_str.split(":")
                    self.leader_address = (l_host, int(l_port_str))
                    print(f"--- [Client] Redirected to leader at {self.leader_address}")
                    continue # The `while` loop will retry with the new leader address
                
                return response

            except (ConnectionRefusedError, OSError, socket.timeout) as e:
                print(f"--- [Client] Connection to {self.leader_address} failed: {type(e).__name__}. Trying another node.")
                # The node is down, remove it from our list of potential contacts for this request
                if self.leader_address in nodes_to_try:
                    nodes_to_try.remove(self.leader_address)
                
                if not nodes_to_try:
                    print("--- [Client] Error: No live nodes in the cluster could be reached.")
                    return {"success": False, "error": "All nodes unreachable"}
                
                # Pick a new random node to try next
                self.leader_address = random.choice(nodes_to_try)
            except Exception as e:
                print(f"--- [Client] An unexpected error occurred: {e}")
                return {"success": False, "error": str(e)}
        
        return {"success": False, "error": "Failed to connect to any node"}

    def set(self, key, value):
        return self._send_request({"type": "CLIENT_SET", "payload": {"key": key, "value": value}})

    def get(self, key):
        return self._send_request({"type": "CLIENT_GET", "payload": {"key": key}})