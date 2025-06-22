import threading
import time
import random
from node import Node, NodeState
from config import CLUSTER_CONFIG
from client import Client

CLUSTER_ADDRESSES = [(info["host"], info["port"]) for info in CLUSTER_CONFIG.values()]

class TestHarness:
    def __init__(self):
        self.nodes = {}
        self.threads = []
        self.results = {"passed": 0, "failed": 0}

    def _start_cluster(self):
        print("--- Starting 5-node cluster...")
        self.nodes = {}
        self.threads = []
        for node_id, node_info in CLUSTER_CONFIG.items():
            peers_config = {p_id: p_info for p_id, p_info in CLUSTER_CONFIG.items() if p_id != node_id}
            node = Node(node_id, node_info["host"], node_info["port"], peers_config)
            self.nodes[node_id] = node
            thread = threading.Thread(target=node.start, daemon=True)
            self.threads.append(thread)
            thread.start()
        time.sleep(0.5) # Allow threads to initialize

    def _stop_cluster(self):
        print("--- Stopping all nodes...")
        for node in self.nodes.values():
            node.stop()
        # All threads are daemonic, so they will exit when the main script does.
        self.nodes = {}

    def _find_leader(self):
        for _ in range(10): # Try for 10 seconds
            leaders = [node for node in self.nodes.values() if node._state == NodeState.LEADER]
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(1)
        return None

    def _run_test(self, test_func, *args):
        test_name = test_func.__name__
        print(f"\n--- RUNNING TEST: {test_name} ---")
        try:
            self._start_cluster()
            result = test_func(*args)
            assert result, "Test assertion failed"
            print(f"--- [PASS] {test_name} ---")
            self.results["passed"] += 1
        except Exception as e:
            print(f"--- [FAIL] {test_name}: {e} ---")
            self.results["failed"] += 1
        finally:
            self._stop_cluster()
            time.sleep(2) # Cooldown period before next test

    def run_all_tests(self):
        self._run_test(self.test_leader_election)
        self._run_test(self.test_basic_set_get)
        self._run_test(self.test_leader_failure_and_recovery)
        self._run_test(self.test_isolated_leader_cannot_commit)
        self._run_test(self.test_concurrent_writes)
        
        print("\n" + "="*40)
        print("--- RIGOROUS TEST GAUNTLET COMPLETE ---")
        print(f"Passed: {self.results['passed']}")
        print(f"Failed: {self.results['failed']}")
        print("="*40)
        if self.results["failed"] == 0:
            print("Conclusion: All tests passed. The system is robust and correct!")
        else:
            print("Conclusion: One or more tests failed.")
        
    def test_leader_election(self):
        leader = self._find_leader()
        print(f"Found leader: {leader.node_id if leader else 'None'}")
        return leader is not None

    def test_basic_set_get(self):
        leader = self._find_leader()
        if not leader: return False
        client = Client(CLUSTER_ADDRESSES)
        
        set_resp = client.set("hello", "world")
        if not (set_resp and set_resp.get("success")):
            raise Exception("SET command failed")

        get_resp = client.get("hello")
        if not (get_resp and get_resp.get("success")):
            raise Exception("GET command failed")

        value = get_resp.get("value")
        print(f"Got value: '{value}'")
        return value == "world"

    def test_leader_failure_and_recovery(self):
        # 1. Find leader and set a value
        initial_leader = self._find_leader()
        if not initial_leader: return False
        print(f"Initial leader is {initial_leader.node_id}")
        client = Client(CLUSTER_ADDRESSES)
        set_resp = client.set("canary", "before_crash")
        if not (set_resp and set_resp.get("success")):
            raise Exception("Initial SET failed")

        # 2. Stop the leader
        print(f"Stopping leader node {initial_leader.node_id}...")
        initial_leader.stop()
        self.nodes.pop(initial_leader.node_id) # Remove from our dictionary

        # 3. Find the new leader
        new_leader = self._find_leader()
        if not new_leader:
            raise Exception("New leader was not elected after failure")
        print(f"New leader is {new_leader.node_id}")

        # 4. Try to get the old value via the new leader
        get_resp = client.get("canary")
        value = get_resp.get("value")
        if value != "before_crash":
            raise Exception(f"Data loss! Expected 'before_crash', got '{value}'")
        print("Data was preserved after leader failure.")
        
        # 5. Try to write a new value
        set_resp_2 = client.set("status", "recovered")
        if not (set_resp_2 and set_resp_2.get("success")):
            raise Exception("SET command failed with new leader")
        print("System can write new data after recovery.")
        return True
        
    def test_isolated_leader_cannot_commit(self):
        # 1. Find leader and isolate it by stopping all other nodes
        leader = self._find_leader()
        if not leader: return False
        print(f"Initial leader is {leader.node_id}. Isolating it...")
        
        followers = [node for node in self.nodes.values() if node.node_id != leader.node_id]
        for follower in followers:
            follower.stop()
            
        time.sleep(2) # Give time for connections to drop

        # 2. Attempt to write to the isolated leader
        client = Client(CLUSTER_ADDRESSES)
        print("Attempting to write 'isolated_write' to isolated leader. This should fail/timeout.")
        set_resp = client.set("isolated_key", "should_fail")
        
        if set_resp and set_resp.get("success"):
            raise Exception("Isolated leader successfully committed a write!")
        
        print("Correctly failed to write to isolated leader.")
        return True
        
    def test_concurrent_writes(self):
        leader = self._find_leader()
        if not leader: return False

        def _concurrent_worker(worker_id, results_list):
            client = Client(CLUSTER_ADDRESSES)
            key = f"concurrent_key_{worker_id}"
            val = f"val_{worker_id}"
            resp = client.set(key, val)
            if resp and resp.get("success"):
                results_list[worker_id] = True

        num_workers = 3
        threads = []
        results = [False] * num_workers
        for i in range(num_workers):
            thread = threading.Thread(target=_concurrent_worker, args=(i, results))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()

        if not all(results):
            raise Exception("One or more concurrent SET commands failed.")
        
        print("All concurrent writes reported success. Verifying values...")
        
        client = Client(CLUSTER_ADDRESSES)
        for i in range(num_workers):
            key = f"concurrent_key_{i}"
            expected_val = f"val_{i}"
            resp = client.get(key)
            if resp.get("value") != expected_val:
                raise Exception(f"Verification failed for {key}. Expected {expected_val}, got {resp.get('value')}")
        
        print("All concurrent values verified correctly.")
        return True

if __name__ == "__main__":
    harness = TestHarness()
    harness.run_all_tests()