"""
Distributed Chat CLI Interface

Command-line interface for P2P distributed chat.
Commands: <text>, /peers, /history, /status, /quit

Usage: python main.py <name> <ip> <port>
"""

from node import Node
from datetime import datetime
from colors import colors
import sys
from typing import NoReturn

def main() -> NoReturn:
    """Initialize node, set callbacks, start command loop."""
    
    if len(sys.argv) < 4:
        print(f"{colors.FAIL}Usage: python main.py <name> <ip> <port>{colors.ENDC}")
        print(f"\nExample:")
        print(f"  python main.py Test 127.0.0.1 5001")
        sys.exit(1)
    
    name = sys.argv[1]
    ip = sys.argv[2]
    port = int(sys.argv[3])
    
    if port < 1024 or port > 65535:
        print(f"{colors.FAIL}Error: Port must be between 1024 and 65535{colors.ENDC}")
        sys.exit(1)
    
    print(f"{colors.OKCYAN}[INFO]{colors.ENDC} Initializing node {colors.BOLD}{name}{colors.ENDC}...")
    node = Node(name, ip, port)
    
    def on_message_received(sender_name: str, text: str, timestamp: float) -> None:
        ts = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
        print(f"\r\033[K{colors.OKCYAN}[{ts}] {colors.OKPURPLE}{sender_name}:{colors.ENDC} {text}")
        print(f"> ", end='', flush=True)
    
    def on_message_sent(text: str, timestamp: float) -> None:
        ts = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
        print(f"\r\033[K{colors.OKCYAN}[{ts}] {colors.OKGREEN}{name}:{colors.ENDC} {text}")
        print(f"> ", end='', flush=True)
    
    node.set_message_callback(on_message_received)
    node.set_sent_callback(on_message_sent)
    
    try:
        node.start()
    except OSError as e:
        print(f"\n{colors.FAIL}[ERROR] Failed to start node: {e}{colors.ENDC}")
        print(f"Possible causes:")
        print(f"  - Port {port} already in use")
        print(f"  - Insufficient permissions")
        sys.exit(1)
    
    print(f"\n{colors.BOLD}{colors.HEADER}=== Distributed Chat System ==={colors.ENDC}")
    print(f"{colors.OKBLUE}Commands:{colors.ENDC}")
    print(f"  {colors.OKGREEN}<text>{colors.ENDC}       - Send message")
    print(f"  {colors.OKGREEN}/peers{colors.ENDC}        - List peers")
    print(f"  {colors.OKGREEN}/history{colors.ENDC}      - View history")
    print(f"  {colors.OKGREEN}/status{colors.ENDC}       - View node status")
    print(f"  {colors.OKGREEN}/quit{colors.ENDC}         - Exit")
    print(f"{colors.BOLD}{colors.HEADER}================================{colors.ENDC}\n")
    
    try:
        while True:
            cmd = input(f"{colors.BOLD}> {colors.ENDC}").strip()
            
            if not cmd:
                continue
            
            if cmd == "/peers":
                print(f"\n{colors.BOLD}{colors.OKBLUE}┌─── Peer List ────────────────────┐{colors.ENDC}")
                print(f"{colors.BOLD}│ Connected:{colors.ENDC}")
                
                # Show self first
                print(f"{colors.BOLD}│{colors.ENDC}  • {colors.OKGREEN}{node.name}{colors.ENDC}")
                
                # Show other peers
                other_peers = [(pid, pinfo) for pid, pinfo in node.peers.items() if pid != node.id]
                for peer_id, peer_info in other_peers:
                    peer_name = peer_info[3] if len(peer_info) > 3 else "Unknown"
                    print(f"{colors.BOLD}│{colors.ENDC}  • {colors.OKPURPLE}{peer_name}{colors.ENDC}")
                
                total_nodes = len(node.peers)
                print(f"{colors.BOLD}│{colors.ENDC}")
                print(f"{colors.BOLD}│ Total nodes:{colors.ENDC} {colors.OKCYAN}{total_nodes}{colors.ENDC}")
                print(f"{colors.BOLD}{colors.OKBLUE}└──────────────────────────────────┘{colors.ENDC}\n")
                
            elif cmd == "/history":
                print(f"\n{colors.BOLD}{colors.OKBLUE}┌─── Message History ──────────────┐{colors.ENDC}")
                if not node.message_history:
                    print(f"{colors.BOLD}│{colors.ENDC} {colors.WARNING}(No messages yet){colors.ENDC}")
                else:
                    for msg in node.message_history:
                        timestamp = datetime.fromtimestamp(msg['timestamp']).strftime('%H:%M:%S')
                        sender_name = msg.get('sender_name', f"Node {msg['sender_id']}")
                        is_me = sender_name == node.name
                        name_color = colors.OKGREEN if is_me else colors.OKPURPLE
                        print(f"{colors.BOLD}│{colors.ENDC} {colors.OKCYAN}[{timestamp}]{colors.ENDC} {name_color}{sender_name}:{colors.ENDC} {msg['text']}")
                print(f"{colors.BOLD}{colors.OKBLUE}└──────────────────────────────────┘{colors.ENDC}\n")
                
            elif cmd == "/status":
                print(f"\n{colors.BOLD}{colors.OKBLUE}┌─── Node Status ──────────────────┐{colors.ENDC}")
                print(f"{colors.BOLD}│ Name:{colors.ENDC} {colors.OKGREEN}{node.name}{colors.ENDC}")
                print(f"{colors.BOLD}│ ID:{colors.ENDC} {colors.OKCYAN}#{node.id if node.id else 'Waiting...'}{colors.ENDC}")
                print(f"{colors.BOLD}│ Address:{colors.ENDC} {node.ip}:{node.port}")
                print(f"{colors.BOLD}│ UDP Port:{colors.ENDC} {node.multicast_port}")
                
                coord_name = "Unknown"
                if node.is_coordinator:
                    coord_name = f"{node.name}"
                elif node.coordinator_id and node.coordinator_id in node.peers:
                    coord_name = node.peers[node.coordinator_id][3]
                print(f"{colors.BOLD}│ Coordinator:{colors.ENDC} {colors.GOLD}{coord_name}{colors.ENDC}")
                
                peer_count = len([p for p in node.peers.keys() if p != node.id])
                print(f"{colors.BOLD}│ Active peers:{colors.ENDC} {colors.OKCYAN}{peer_count}{colors.ENDC}")
                print(f"{colors.BOLD}{colors.OKBLUE}└──────────────────────────────────┘{colors.ENDC}\n")
                
            elif cmd == "/quit":
                print(f"{colors.WARNING}[INFO]{colors.ENDC} Exiting chat...")
                break
            
            elif cmd.startswith("/"):
                print(f"{colors.WARNING}[WARNING]{colors.ENDC} Command '{cmd}' does not exist. Type /peers, /history, /status, or /quit")
            
            else:
                node.send_chat_message(cmd)
    
    except KeyboardInterrupt:
        print(f"\n{colors.WARNING}[INFO]{colors.ENDC} Interrupted by user")
    
    finally:
        node.leave_network()
        print(f"{colors.OKGREEN}Exiting...{colors.ENDC}")

if __name__ == "__main__":
    main()
