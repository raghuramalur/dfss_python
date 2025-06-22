import time
from config import CLUSTER_CONFIG
from client import Client

# Create a simple list of addresses from the main config
CLUSTER_ADDRESSES = [(info["host"], info["port"]) for info in CLUSTER_CONFIG.values()]

def main():
    print("--- Raft Interactive Client ---")
    print("Commands: set <key> <value>, get <key>, exit")
    time.sleep(2) # Give cluster a moment to elect a leader on first run
    client = Client(CLUSTER_ADDRESSES)
    
    while True:
        try:
            cmd_line = input(">> ").strip().split()
            if not cmd_line: continue
            
            command = cmd_line[0].lower()
            if command == "exit":
                break
            elif command == "set" and len(cmd_line) >= 3:
                response = client.set(cmd_line[1], " ".join(cmd_line[2:]))
                if response and response.get("success"):
                    print("OK")
                else:
                    print(f"Error: {response.get('error', 'SET command failed or timed out.')}")
            elif command == "get" and len(cmd_line) == 2:
                response = client.get(cmd_line[1])
                if response and response.get("success"):
                    print(f"> {response.get('value', 'nil')}")
                else:
                    print(f"Error: {response.get('error', 'GET command failed.')}")
            else:
                print("Invalid command.")
        except Exception as e:
            print(f"A client error occurred: {e}")

if __name__ == "__main__":
    main()