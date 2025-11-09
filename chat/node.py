"""
P2P Distributed Chat System

TCP for P2P messaging, UDP multicast for discovery.
Bully algorithm for coordinator election.
Heartbeat-based failure detection.
"""

import socket
import struct
import time
import random
import threading
import json
from typing import Dict, Tuple, List, Set, Callable, Optional, Any
from colors import colors

MULTICAST_GROUP = "224.0.0.1"
MULTICAST_DISCOVERY_PORT = 5007
MULTICAST_PORT_RANGE_START = 5100
MULTICAST_PORT_RANGE_END = 5200


class Node:
    """P2P chat node with coordinator election and failure detection."""

    def __init__(self, name: str, ip: str, port: int) -> None:
        self.name = name
        self.ip = ip
        self.port = port
        self.id: Optional[int] = None
        
        self.multicast_group = MULTICAST_GROUP
        self.multicast_port: Optional[int] = None
        
        self.is_coordinator = False
        self.coordinator_id: Optional[int] = None
        self.next_id = 1
        
        self.peers: Dict[int, Tuple[str, int, int, str]] = {}
        self.used_multicast_ports: Set[int] = set()
        self.available_udp_ports: Set[int] = set(range(MULTICAST_PORT_RANGE_START, MULTICAST_PORT_RANGE_END + 1))
        
        self.message_history: List[Dict] = []
        self.last_heartbeat: Dict[int, float] = {}
        
        self.tcp_sock: Optional[socket.socket] = None
        self.udp_sock: Optional[socket.socket] = None
        self.discovery_sock: Optional[socket.socket] = None
        
        self.running = False
        self.lock = threading.Lock()
        
        self.election_in_progress = False
        self.election_responses: List[int] = []
        
        self.message_callback: Optional[Callable] = None
        self.sent_callback: Optional[Callable] = None

    def set_message_callback(self, callback: Callable[[str, str, float], None]) -> None:
        self.message_callback = callback

    def set_sent_callback(self, callback: Callable[[str, float], None]) -> None:
        self.sent_callback = callback

    def start(self) -> None:
        """Start node: bind sockets, join multicast, start threads, join network."""
        self.running = True

        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_sock.bind((self.ip, self.port))
        self.tcp_sock.listen(10)

        self.discovery_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.discovery_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.discovery_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
        self.discovery_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        self.discovery_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        self.discovery_sock.bind(("", MULTICAST_DISCOVERY_PORT))
        
        mreq = struct.pack('4sl', socket.inet_aton(self.multicast_group), socket.INADDR_ANY)
        self.discovery_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        self.multicast_port = random.randint(MULTICAST_PORT_RANGE_START, MULTICAST_PORT_RANGE_END)
        
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
        self.udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        self.udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        self.udp_sock.bind(("", self.multicast_port))
        self.udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        print(f"{colors.OKGREEN}[INFO]{colors.ENDC} Node {colors.BOLD}{self.name}{colors.ENDC} started at TCP:{self.ip}:{self.port} / UDP:{self.multicast_port}")
        
        threading.Thread(target=self._listen_tcp, daemon=True).start()
        threading.Thread(target=self._listen_udp, daemon=True).start()
        threading.Thread(target=self._listen_discovery, daemon=True).start()
        threading.Thread(target=self._heartbeat_sender, daemon=True).start()
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()
        
        time.sleep(1)
        self.join_network()
    
    def join_network(self) -> None:
        """Send JOIN_REQUEST, wait 3s for coordinator, or become coordinator."""
        message = {
            "type": "JOIN_REQUEST",
            "ip": self.ip,
            "port": self.port,
            "udp_port": self.multicast_port,
            "name": self.name
        }
        self._send_multicast(message)
        print(f"{colors.OKCYAN}[INFO]{colors.ENDC} Looking for coordinator...")
        
        time.sleep(3)
        
        if self.id is None:
            self.id = 1
            self.is_coordinator = True
            self.coordinator_id = self.id
            self.next_id = 2
            self.used_multicast_ports.add(self.multicast_port)
            self.available_udp_ports.discard(self.multicast_port)
            self.peers[self.id] = (self.ip, self.port, self.multicast_port, self.name)
            print(f"{colors.GOLD}[INFO]{colors.ENDC} No coordinator found. {colors.BOLD}{self.name}{colors.ENDC} became {colors.GOLD}COORDINATOR{colors.ENDC}")
    
    def _listen_tcp(self) -> None:
        """Accept TCP connections and spawn handlers."""
        while self.running:
            try:
                conn, address = self.tcp_sock.accept()
                threading.Thread(target=self._handle_tcp_connection, args=(conn, address), daemon=True).start()
            except Exception as e:
                if self.running:
                    print(f"{colors.FAIL}[ERROR]{colors.ENDC} TCP server error: {e}")
    
    def _handle_tcp_connection(self, conn: socket.socket, address: Tuple[str, int]) -> None:
        """Receive and process TCP message."""
        try:
            data = conn.recv(4096)
            if data:
                self._handle_message(json.loads(data.decode()), address)
        except Exception as e:
            print(f"{colors.FAIL}[ERROR]{colors.ENDC} Error processing TCP: {e}")
        finally:
            conn.close()
    
    def _listen_udp(self) -> None:
        """Receive UDP multicast messages."""
        while self.running:
            try:
                data, address = self.udp_sock.recvfrom(4096)
                self._handle_message(json.loads(data.decode()), address)
            except Exception as e:
                if self.running:
                    print(f"{colors.FAIL}[ERROR]{colors.ENDC} UDP error: {e}")
    
    def _listen_discovery(self) -> None:
        """Receive JOIN_REQUEST on fixed discovery port."""
        while self.running:
            try:
                data, address = self.discovery_sock.recvfrom(4096)
                message = json.loads(data.decode())
                if message.get("type") == "JOIN_REQUEST":
                    self._handle_message(message, address)
            except Exception as e:
                if self.running:
                    print(f"{colors.FAIL}[ERROR]{colors.ENDC} Discovery error: {e}")
    
    def _handle_message(self, message: Dict[str, Any], address: Tuple[str, int]) -> None:
        """Route message to appropriate handler."""
        msg_type = message.get("type")
        
        if msg_type == "JOIN_REQUEST" and self.is_coordinator:
            self._handle_join_request(message)
        elif msg_type == "JOIN_RESPONSE":
            self._handle_join_response(message)
        elif msg_type == "NEW_NODE":
            self._handle_new_node(message)
        elif msg_type == "HEARTBEAT":
            sender_id = message.get("sender_id")
            if sender_id and sender_id != self.id:
                with self.lock:
                    self.last_heartbeat[sender_id] = time.time()
                    if message.get("is_coordinator"):
                        self.coordinator_id = sender_id
        elif msg_type == "CHAT_MESSAGE":
            self._handle_chat_message(message)
        elif msg_type == "ELECTION":
            self._handle_election(message)
        elif msg_type == "ELECTION_OK":
            sender_id = message.get("sender_id")
            if sender_id:
                self.election_responses.append(sender_id)
        elif msg_type == "COORDINATOR":
            self._handle_coordinator_announcement(message)
        elif msg_type == "NODE_LEFT":
            self._handle_node_left(message)
    
    def _handle_join_request(self, message: Dict[str, Any]) -> None:
        """Coordinator: assign ID and UDP port from pool, send JOIN_RESPONSE."""
        peer_ip = message["ip"]
        peer_port = message["port"]
        peer_udp_port = message.get("udp_port")
        peer_name = message.get("name", "Unknown")
        
        with self.lock:
            if peer_udp_port in self.available_udp_ports:
                assigned_udp_port = peer_udp_port
                self.available_udp_ports.discard(assigned_udp_port)
            else:
                if not self.available_udp_ports:
                    print(f"\r\033[K{colors.FAIL}[ERROR]{colors.ENDC} No UDP ports available")
                    print(f"> ", end='', flush=True)
                    return
                assigned_udp_port = self.available_udp_ports.pop()
            
            new_id = self.next_id
            self.next_id += 1
            
            self.peers[new_id] = (peer_ip, peer_port, assigned_udp_port, peer_name)
            self.used_multicast_ports.add(assigned_udp_port)
            
            response = {
                "type": "JOIN_RESPONSE",
                "assigned_id": new_id,
                "assigned_udp_port": assigned_udp_port,
                "coordinator_id": self.id,
                "coordinator_name": self.name,
                "peers": {pid: list(pinfo) for pid, pinfo in self.peers.items()},
                "used_ports": list(self.used_multicast_ports),
                "available_ports": list(self.available_udp_ports)
            }
        
        self._send_to(response, peer_ip, peer_port)
        
        self._broadcast({
            "type": "NEW_NODE",
            "node_id": new_id,
            "ip": peer_ip,
            "port": peer_port,
            "udp_port": assigned_udp_port,
            "name": peer_name
        })
        
        print(f"\r\033[K{colors.OKGREEN}[INFO]{colors.ENDC} {colors.BOLD}{peer_name}{colors.ENDC} joined")
        print(f"> ", end='', flush=True)
    
    def _handle_join_response(self, message: Dict[str, Any]) -> None:
        """Receive ID, port assignment, peer list from coordinator."""
        self.id = message["assigned_id"]
        assigned_udp_port = message.get("assigned_udp_port")
        
        if assigned_udp_port and assigned_udp_port != self.multicast_port:
            self._reconfigure_udp_socket(assigned_udp_port)
        
        self.coordinator_id = message["coordinator_id"]
        
        # Add all peers including self
        for peer_id, peer_info in message["peers"].items():
            peer_id = int(peer_id)
            self.peers[peer_id] = tuple(peer_info)
        
        self.used_multicast_ports = set(message.get("used_ports", []))
        if "available_ports" in message:
            self.available_udp_ports = set(message["available_ports"])
        
        coord_name = message.get("coordinator_name", f"ID {self.coordinator_id}")
        print(f"{colors.OKGREEN}[INFO]{colors.ENDC} Joined network. Coordinator: {colors.GOLD}{coord_name}{colors.ENDC}")

    def _reconfigure_udp_socket(self, new_port: int) -> None:
        """Close and recreate UDP socket with new port."""
        if self.udp_sock:
            self.udp_sock.close()
        
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        self.udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        self.udp_sock.bind(("", new_port))
        
        mreq = struct.pack('4sl', socket.inet_aton(self.multicast_group), socket.INADDR_ANY)
        self.udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        self.multicast_port = new_port
    
    def _handle_new_node(self, message: Dict[str, Any]) -> None:
        """Add new node to peer list."""
        node_id = message["node_id"]
        
        if self.id is not None and node_id != self.id:
            node_name = message.get("name", "Unknown")
            self.peers[node_id] = (message["ip"], message["port"], message.get("udp_port"), node_name)
            self.used_multicast_ports.add(message.get("udp_port"))
            print(f"\r\033[K{colors.OKGREEN}[INFO]{colors.ENDC} {colors.BOLD}{node_name}{colors.ENDC} joined")
            print(f"> ", end='', flush=True)

    def _heartbeat_sender(self) -> None:
        """Send heartbeat every 2 seconds."""
        while self.running:
            if self.id is not None:
                self._broadcast({
                    "type": "HEARTBEAT",
                    "sender_id": self.id,
                    "is_coordinator": self.is_coordinator
                })
            time.sleep(2)
    
    def _heartbeat_monitor(self) -> None:
        """Detect failed nodes (10s timeout), start election if coordinator fails."""
        while self.running:
            time.sleep(5)
            if self.id is None:
                continue
                
            current_time = time.time()
            failed_nodes = []
            
            with self.lock:
                for peer_id in list(self.last_heartbeat.keys()):
                    if current_time - self.last_heartbeat[peer_id] > 10:
                        failed_nodes.append(peer_id)
                        del self.last_heartbeat[peer_id]
                        
                        if peer_id in self.peers:
                            ip, port, udp_port, peer_name = self.peers[peer_id]
                            
                            self.used_multicast_ports.discard(udp_port)
                            if self.is_coordinator:
                                self.available_udp_ports.add(udp_port)
                            
                            del self.peers[peer_id]
                            print(f"\r\033[K{colors.WARNING}[WARNING]{colors.ENDC} {peer_name} disconnected")
                            print(f"> ", end='', flush=True)
        
            if self.coordinator_id in failed_nodes and not self.is_coordinator:
                print(f"\r\033[K{colors.FAIL}[WARNING]{colors.ENDC} Coordinator failed. Starting election...")
                print(f"> ", end='', flush=True)
                self.start_election()
    
    def start_election(self) -> None:
        """Bully algorithm: send ELECTION to higher IDs, wait 3s for OK, become coordinator if none."""
        if self.election_in_progress:
            return
        
        self.election_in_progress = True
        self.election_responses = []
        
        print(f"\r\033[K{colors.GOLD}[ELECTION]{colors.ENDC} {self.name} starting election")
        print(f"> ", end='', flush=True)
        
        with self.lock:
            higher_nodes = [(pid, self.peers[pid]) for pid in self.peers.keys() if pid > self.id]
        
        if not higher_nodes:
            self._become_coordinator()
        else:
            for peer_id, (ip, port, _, _) in higher_nodes:
                self._send_to({"type": "ELECTION", "sender_id": self.id}, ip, port)
            
            time.sleep(3)
            
            if not self.election_responses:
                self._become_coordinator()
        
        self.election_in_progress = False
    
    def _handle_election(self, message: Dict[str, Any]) -> None:
        """Respond OK to lower ID, start own election."""
        sender_id = message["sender_id"]
        
        if sender_id < self.id:
            with self.lock:
                if sender_id in self.peers:
                    ip, port, _, _ = self.peers[sender_id]
                    self._send_to({"type": "ELECTION_OK", "sender_id": self.id}, ip, port)
            
            if not self.election_in_progress:
                threading.Thread(target=self.start_election, daemon=True).start()
    
    def _become_coordinator(self) -> None:
        """Become coordinator: rebuild port pool, announce to network."""
        with self.lock:
            self.is_coordinator = True
            self.coordinator_id = self.id
            self.next_id = max(list(self.peers.keys()) + [self.id]) + 1
            
            if self.id not in self.peers:
                self.peers[self.id] = (self.ip, self.port, self.multicast_port, self.name)
            
            self.available_udp_ports = set(range(MULTICAST_PORT_RANGE_START, MULTICAST_PORT_RANGE_END + 1))
            self.used_multicast_ports.clear()
            
            for _, _, udp_port, _ in self.peers.values():
                self.used_multicast_ports.add(udp_port)
                self.available_udp_ports.discard(udp_port)
        
        print(f"\r\033[K{colors.GOLD}[ELECTION]{colors.ENDC} {self.name} is the new {colors.GOLD}COORDINATOR{colors.ENDC}")
        print(f"> ", end='', flush=True)
        
        self._broadcast({"type": "COORDINATOR", "coordinator_id": self.id, "coordinator_name": self.name})
    
    def _handle_coordinator_announcement(self, message: Dict[str, Any]) -> None:
        """Update coordinator ID from announcement."""
        new_coord_id = message["coordinator_id"]
        
        with self.lock:
            if new_coord_id != self.coordinator_id:
                self.coordinator_id = new_coord_id
                if self.coordinator_id != self.id:
                    self.is_coordinator = False
                    self.election_in_progress = False
        
        coord_name = message.get("coordinator_name", f"ID {new_coord_id}")
        print(f"\r\033[K{colors.GOLD}[INFO]{colors.ENDC} New coordinator: {coord_name}")
        print(f"> ", end='', flush=True)
    
    def send_chat_message(self, text: str) -> None:
        """Broadcast message to all peers."""
        if self.id is None:
            print("[ERROR] Not yet in network")
            return
        
        timestamp = time.time()
        message = {
            "type": "CHAT_MESSAGE",
            "sender_id": self.id,
            "sender_name": self.name,
            "text": text,
            "timestamp": timestamp
        }
        self._broadcast(message)
        self._add_to_history(message)
        
        if self.sent_callback:
            self.sent_callback(text, timestamp)
    
    def _handle_chat_message(self, message: Dict[str, Any]) -> None:
        """Add received message to history, trigger callback."""
        if message.get('sender_id') == self.id:
            return
            
        self._add_to_history(message)
        
        if self.message_callback:
            sender_name = message.get('sender_name', f"Node {message['sender_id']}")
            self.message_callback(sender_name, message['text'], message['timestamp'])
    
    def _add_to_history(self, message: Dict[str, Any]) -> None:
        """Add message to history with deduplication."""
        msg_id = f"{message['sender_name']}_{message['timestamp']}"
        
        with self.lock:
            if not any(f"{m['sender_name']}_{m['timestamp']}" == msg_id for m in self.message_history):
                self.message_history.append(message)
                self.message_history.sort(key=lambda x: x['timestamp'])
    
    def _handle_node_left(self, message: Dict[str, Any]) -> None:
        """Remove node from peers, return UDP port to pool."""
        node_id = message["node_id"]
        
        with self.lock:
            if node_id in self.peers:
                _, _, udp_port, peer_name = self.peers[node_id]
                
                self.used_multicast_ports.discard(udp_port)
                if self.is_coordinator:
                    self.available_udp_ports.add(udp_port)
                
                del self.peers[node_id]
                print(f"\r\033[K{colors.OKCYAN}[INFO]{colors.ENDC} {peer_name} left")
                print(f"> ", end='', flush=True)
    
    def leave_network(self) -> None:
        """Broadcast NODE_LEFT, close sockets."""
        if self.id is not None:
            self._broadcast({"type": "NODE_LEFT", "node_id": self.id})
        
        self.running = False
        if self.tcp_sock:
            self.tcp_sock.close()
        if self.udp_sock:
            self.udp_sock.close()
        if self.discovery_sock:
            self.discovery_sock.close()

    def _send_multicast(self, message: Dict[str, Any]) -> None:
        """Send UDP multicast to discovery port or all peer ports."""
        data = json.dumps(message).encode()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        try:
            if message.get("type") == "JOIN_REQUEST":
                sock.sendto(data, (self.multicast_group, MULTICAST_DISCOVERY_PORT))
            elif self.used_multicast_ports:
                for port in self.used_multicast_ports:
                    sock.sendto(data, (self.multicast_group, port))
        except Exception as e:
            print(f"[ERROR] Multicast error: {e}")
        finally:
            sock.close()

    def _send_to(self, message: Dict[str, Any], ip: str, port: int) -> None:
        """Send TCP message to specific address."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((ip, port))
            sock.sendall(json.dumps(message).encode())
            sock.close()
        except Exception as e:
            print(f"[ERROR] TCP error to {ip}:{port}: {e}")
    
    def _broadcast(self, message: Dict[str, Any]) -> None:
        """Send message to all known peers via TCP."""
        for peer_id, (ip, port, _, _) in list(self.peers.items()):
            self._send_to(message, ip, port)
