# raft_kv_store/node.py

import json
import threading
import random
import time
from enum import Enum
from p2p.tcp_transport import TCPTransport

class KeyValueStore:
    def __init__(self): self._data = {}
    def set(self, key, value): self._data[key] = value
    def get(self, key): return self._data.get(key)
class Log:
    def __init__(self): self._entries = []
    def append(self, entry): self._entries.append(entry)
    def __len__(self): return len(self._entries)
    def __getitem__(self, index): return self._entries[index]
class NodeState(Enum):
    FOLLOWER = 1; CANDIDATE = 2; LEADER = 3

class Node:
    def __init__(self, node_id, host, port, peers_config):
        self.node_id = node_id; self.host = host; self.port = port
        self.peers_config = peers_config
        self.majority_count = (len(self.peers_config) + 1) // 2 + 1
        self.current_term = 0; self.voted_for = None; self.log = Log()
        self._state = NodeState.FOLLOWER; self.commit_index = -1; self.last_applied = -1
        self.current_leader_id = None; self._leader_state = {}; self._lock = threading.RLock()
        self.transport = TCPTransport(self.host, self.port, self.node_id)
        self.transport.on_peer_message = self._handle_peer_message
        self.transport.on_client_message = self._handle_client_message
        self.kv_store = KeyValueStore(); self._election_timer = None; self._heartbeat_timer = None
        self._running = True; self._commit_events = {}

    def start(self):
        self.transport.start(); self._reset_election_timer(); time.sleep(1)
        # The canonical dialing rule is no longer strictly necessary with the new transport,
        # but it's good practice to prevent initial connection storms.
        for peer_id, peer_info in self.peers_config.items():
            if peer_id > self.node_id:
                self.transport.dial(peer_id, peer_info["host"], peer_info["port"])

    def stop(self):
        self._running = False
        if self._election_timer: self._election_timer.cancel()
        if self._heartbeat_timer: self._heartbeat_timer.cancel()
        self.transport.stop()

    def _handle_client_message(self, message, conn):
        if not self._running: return
        msg_type = message.get("type")
        if msg_type == "CLIENT_SET": self._handle_client_set(message, conn)
        elif msg_type == "CLIENT_GET": self._handle_client_get(message, conn)
        else: conn.close()
    
    def _handle_peer_message(self, message, from_peer_id):
        if not self._running: return
        term = message.get("term", 0)
        with self._lock:
            if term > self.current_term:
                self.current_term = term; self._state = NodeState.FOLLOWER; self.voted_for = None
                if self._heartbeat_timer: self._heartbeat_timer.cancel()
        msg_type = message.get("type")
        if msg_type == "REQUEST_VOTE": self._handle_request_vote(message, from_peer_id)
        elif msg_type == "REQUEST_VOTE_RESPONSE": self._handle_request_vote_response(message)
        elif msg_type == "APPEND_ENTRIES": self._handle_append_entries(message, from_peer_id)
        elif msg_type == "APPEND_ENTRIES_RESPONSE": self._handle_append_entries_response(message, from_peer_id)

    def _send_client_response(self, conn, response):
        data = json.dumps(response).encode('utf-8')
        len_bytes = len(data).to_bytes(4, 'big')
        try: conn.sendall(len_bytes + data)
        except: pass
        finally: conn.close()

    def _handle_client_set(self, message, conn):
        with self._lock:
            if self._state != NodeState.LEADER:
                leader_info = self.peers_config.get(self.current_leader_id)
                leader_addr = f"{leader_info['host']}:{leader_info['port']}" if leader_info else None
                self._send_client_response(conn, {"success": False, "leader": leader_addr})
                return
            log_entry = {"term": self.current_term, "command": message["payload"]}
            self.log.append(log_entry)
            entry_index = len(self.log) - 1
            commit_event = threading.Event()
            self._commit_events[entry_index] = commit_event

            # Force AppendEntries broadcast
            for peer_id in self.peers_config:
                self._send_append_entries_to_peer(peer_id)

        success = commit_event.wait(timeout=5.0)
        with self._lock:
            if entry_index in self._commit_events:
                del self._commit_events[entry_index]
        self._send_client_response(conn, {"success": success})


    def _handle_client_get(self, message, conn):
        with self._lock:
            if self._state != NodeState.LEADER:
                leader_info = self.peers_config.get(self.current_leader_id)
                leader_addr = f"{leader_info['host']}:{leader_info['port']}" if leader_info else None
                self._send_client_response(conn, {"success": False, "leader": leader_addr})
                return
            value = self.kv_store.get(message["payload"]["key"])
        self._send_client_response(conn, {"success": True, "value": value})

    def _apply_log_entries(self):
        with self._lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied]
                # Skip applying no-op entries to the KV store
                if entry["command"]["key"] == "NO_OP" and entry["command"]["value"] is None:
                    continue
                self.kv_store.set(entry["command"]["key"], entry["command"]["value"])
                if self.last_applied in self._commit_events:
                    self._commit_events[self.last_applied].set()

    def _reset_election_timer(self):
        if not self._running: return
        if self._election_timer: self._election_timer.cancel()
        self._election_timer = threading.Timer(random.uniform(3, 6), self._handle_election_timeout)
        self._election_timer.start()

    def _handle_election_timeout(self):
        with self._lock:
            if self._running and self._state != NodeState.LEADER: self._start_election()
            
    def _start_election(self):
        self._state = NodeState.CANDIDATE; self.current_term += 1; self.voted_for = self.node_id
        self.votes_received = 1; self._reset_election_timer()
        
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else -1
        
        self.transport.broadcast({
            "type": "REQUEST_VOTE", 
            "term": self.current_term, 
            "candidate_id": self.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term
        })

    def _handle_request_vote(self, message, from_peer_id):
        with self._lock:
            # Rule 1: Reply false if term < currentTerm
            if message["term"] < self.current_term:
                response = {"type": "REQUEST_VOTE_RESPONSE", "term": self.current_term, "vote_granted": False}
                self.transport.send(from_peer_id, response)
                return

            # Note: The logic to step down if message['term'] > self.current_term
            # is already handled in _handle_peer_message, which runs before this.

            # Rule 2: If votedFor is null or candidateId, and candidate's log is at least as
            # up-to-date as receiver's log, grant vote
            can_vote_this_term = self.voted_for is None or self.voted_for == message["candidate_id"]

            my_last_log_index = len(self.log) - 1
            my_last_log_term = self.log[my_last_log_index]['term'] if my_last_log_index >= 0 else -1
            candidate_last_log_term = message["last_log_term"]
            candidate_last_log_index = message["last_log_index"]
            
            # The core of the Raft safety rule for leader election
            candidate_is_up_to_date = (candidate_last_log_term > my_last_log_term) or \
                                      (candidate_last_log_term == my_last_log_term and \
                                       candidate_last_log_index >= my_last_log_index)

            vote_granted = False
            if can_vote_this_term and candidate_is_up_to_date:
                self.voted_for = message["candidate_id"]
                self._reset_election_timer() # Granting a vote resets the election timer
                vote_granted = True
                
            response = {"type": "REQUEST_VOTE_RESPONSE", "term": self.current_term, "vote_granted": vote_granted}
            self.transport.send(from_peer_id, response)


    def _handle_request_vote_response(self, message):
        with self._lock:
            if self._state == NodeState.CANDIDATE and message["term"] == self.current_term and message["vote_granted"]:
                self.votes_received += 1
                if self.votes_received >= self.majority_count: self._become_leader()

    def _become_leader(self):
        if self._state != NodeState.CANDIDATE: return
        print(f"\n!!!!!!!!!!!! Node-{self.node_id}: Became LEADER for term {self.current_term} !!!!!!!!!!!!\n")
        self._state = NodeState.LEADER; self.current_leader_id = self.node_id
        self._leader_state['next_index'] = {peer_id: len(self.log) for peer_id in self.peers_config}
        self._leader_state['match_index'] = {peer_id: -1 for peer_id in self.peers_config}
        if self._election_timer: self._election_timer.cancel()

        # Add a no-op entry to the log on election to help commit lingering entries from previous terms
        log_entry = {"term": self.current_term, "command": {"key": "NO_OP", "value": None}}
        self.log.append(log_entry)

        self._send_heartbeats()

    def _send_heartbeats(self):
        with self._lock:
            if self._state != NodeState.LEADER or not self._running: return
            for peer_id in self.peers_config: self._send_append_entries_to_peer(peer_id)
            self._heartbeat_timer = threading.Timer(1.5, self._send_heartbeats)
            self._heartbeat_timer.start()

    def _send_append_entries_to_peer(self, peer_id):
        next_idx = self._leader_state['next_index'][peer_id]
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else -1
        entries = self.log._entries[next_idx:]
        message = {"type": "APPEND_ENTRIES", "term": self.current_term, "leader_id": self.node_id,
                   "leader_addr": f"{self.host}:{self.port}", "prev_log_index": prev_log_index,
                   "prev_log_term": prev_log_term, "entries": entries, "leader_commit": self.commit_index }
        self.transport.send(peer_id, message)

    def _handle_append_entries(self, message, from_peer_id):
        with self._lock:
            response = {"type": "APPEND_ENTRIES_RESPONSE", "term": self.current_term, "success": False}
            if message["term"] < self.current_term: self.transport.send(from_peer_id, response); return
            self.current_leader_id = message.get("leader_id")
            if self._state == NodeState.CANDIDATE: self._state = NodeState.FOLLOWER
            self._reset_election_timer()
            prev_log_index = message["prev_log_index"]
            if prev_log_index >= 0 and (len(self.log) <= prev_log_index or self.log[prev_log_index]['term'] != message["prev_log_term"]):
                self.transport.send(from_peer_id, response); return
            self.log._entries = self.log._entries[:prev_log_index + 1] + message["entries"]
            if message["leader_commit"] > self.commit_index:
                self.commit_index = min(message["leader_commit"], len(self.log) - 1)
                self._apply_log_entries()
            response["success"] = True; response["match_index"] = len(self.log) - 1
            self.transport.send(from_peer_id, response)

    def _handle_append_entries_response(self, message, from_peer_id):
        with self._lock:
            if self._state != NodeState.LEADER or message["term"] != self.current_term: return
            if message["success"]:
                self._leader_state['next_index'][from_peer_id] = message["match_index"] + 1
                self._leader_state['match_index'][from_peer_id] = message["match_index"]
                self._update_commit_index()
            else:
                self._leader_state['next_index'][from_peer_id] -= 1

    def _update_commit_index(self):
        for N in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[N]['term'] == self.current_term:
                count = 1 + sum(1 for peer_id in self.peers_config if self._leader_state['match_index'][peer_id] >= N)
                if count >= self.majority_count: self.commit_index = N; break
        self._apply_log_entries()