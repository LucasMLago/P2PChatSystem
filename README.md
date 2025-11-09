# Distributed P2P Chat System

A decentralized peer-to-peer chat system implemented in Python using TCP/UDP protocols, featuring automatic network discovery, coordinator election via Bully algorithm, and fault tolerance through heartbeat monitoring.

## Features

- Decentralized architecture with no central server
- Automatic peer discovery using UDP multicast
- Leader election using Bully algorithm
- Failure detection and automatic recovery
- Message history synchronization for new nodes
- Thread-safe concurrent operations
- Dynamic UDP port allocation managed by coordinator

## Requirements

- Python 3.7+
- Standard library only
- Local network supporting UDP multicast

## Installation and Usage

```bash
cd distributed_chat/chat

# Terminal 1 - First node becomes coordinator
python main.py Lucas 127.0.0.1 5001

# Terminal 2 - Additional nodes join network
python main.py Julia 127.0.0.1 5002
python main.py Maria 127.0.0.1 5003
```

### Available Commands

| Command | Description |
|---------|-------------|
| `<text>` | Send message to all nodes |
| `/peers` | List all connected nodes (including yourself) |
| `/history` | View message history |
| `/status` | Show node information |
| `/quit` | Exit gracefully |

**Note:** Any command starting with `/` that doesn't match the above will show a warning message.

## Technical Architecture

### Communication Protocols

**TCP (Transmission Control Protocol)**
- Reliable point-to-point communication
- Chat messages between peers
- Control messages (election, coordinator announcements)
- Node join/leave notifications

**UDP Multicast**
- Network discovery
- Initial join requests
- Heartbeat monitoring
- Quick broadcast to all peers

### Network Configuration

```python
Multicast Group:    224.0.0.1
Discovery Port:     5007 (fixed)
UDP Port Range:     5100-5200 (dynamic)
TCP Ports:          User-defined (unique per node)
```

### Port Management

The coordinator maintains a centralized UDP port pool:
- Available ports: 5100-5200 (101 ports total)
- Allocation: Coordinator assigns from available pool when node joins
- Deallocation: Port returned to pool when node leaves or fails
- Prevents port conflicts and supports up to 101 concurrent nodes

### Message Types

#### Control Messages

| Type | Description | Transport |
|------|-------------|-----------|
| `JOIN_REQUEST` | Node requests network entry | UDP Multicast |
| `JOIN_RESPONSE` | Coordinator assigns ID and port | TCP |
| `NEW_NODE` | Announce new node | TCP |
| `HEARTBEAT` | Health check | UDP Multicast |
| `ELECTION` | Initiate coordinator election | TCP |
| `ELECTION_OK` | Election response | TCP |
| `COORDINATOR` | New coordinator announcement | TCP |
| `NODE_LEFT` | Node departure notification | TCP |

#### Data Messages

| Type | Description | Transport |
|------|-------------|-----------|
| `CHAT_MESSAGE` | User message | TCP |

## Core Processes

### Network Discovery and Join

**Node Startup:**
1. Node binds TCP socket and joins UDP multicast group
2. Sends `JOIN_REQUEST` via multicast discovery port (5007)
3. Waits 3 seconds for coordinator response
4. If no response: becomes coordinator (assigns self ID=1)
5. If response received: gets assigned ID, UDP port, peer list, and message history
6. Reconfigures UDP socket if port changed
7. Begins sending heartbeats

**Coordinator Processing:**
1. Receives `JOIN_REQUEST`
2. Acquires lock on port pool
3. Allocates available UDP port from pool
4. Assigns unique ID (auto-increment)
5. Adds new node to peer list
6. Sends `JOIN_RESPONSE` to new node
7. Broadcasts `NEW_NODE` announcement to all peers

### Coordinator Election (Bully Algorithm)

**Trigger Conditions:**
- Coordinator failure (no heartbeat for 10 seconds)
- Network partition recovery

**Election Process:**
1. Node detects coordinator failure
2. Sends `ELECTION` message to all nodes with higher ID
3. Waits 3 seconds for `ELECTION_OK` responses
4. If receives `ELECTION_OK`: stands down
5. If no `ELECTION_OK`: becomes coordinator
6. New coordinator broadcasts `COORDINATOR` announcement

**Example:**
```
Network: Node1(ID=1), Node2(ID=2), Node3(ID=3), Node4(ID=4)
Current Coordinator: Node4

Node4 crashes:
- Node3 sends ELECTION to Node4 (no response)
- Node3 has no higher-ID nodes, becomes coordinator
- Node2 sends ELECTION to Node3 and Node4
- Receives ELECTION_OK from Node3, stands down
- Node1 sends ELECTION to all higher nodes
- Receives ELECTION_OK, stands down

Result: Node3 becomes coordinator
```

### Failure Detection

**Heartbeat System:**
- Send interval: 2 seconds
- Timeout: 10 seconds (5 missed heartbeats)
- Content: Node ID, coordinator status, timestamp

**Failure Handling:**
1. Monitor detects missing heartbeats (>10 seconds)
2. Remove failed node from peer list
3. Return UDP port to available pool
4. If coordinator failed: initiate election
5. Update UI with disconnection notice

### Message Routing

**Sending:**
1. Create `CHAT_MESSAGE` with sender info, text, and timestamp
2. Add to local message history
3. Broadcast to all peers via TCP (iterates through peers list)
4. Trigger UI callback

**Receiving:**
1. Receive `CHAT_MESSAGE` from peer
2. Verify not from self (avoid duplication)
3. Add to local history with deduplication
4. Trigger UI callback
5. Sort history chronologically

**History Synchronization:**
- New nodes receive full history in `JOIN_RESPONSE`
- Each node maintains independent copy including themselves in peers list
- Deduplication by `sender_name + timestamp`

## Project Structure

```
distributed_chat/
├── chat/
│   ├── main.py      # CLI interface
│   ├── node.py      # P2P implementation
│   └── colors.py    # Terminal colors
└── README.md
```

### Component Responsibilities

**main.py** - User Interface Layer
- Command-line argument parsing
- User input handling and command routing
- Message formatting with timestamps
- Callback registration for UI updates

**node.py** - Network Layer
- TCP/UDP socket management
- Peer management and tracking
- Coordinator election logic
- Message routing and history
- Failure detection via heartbeats
- Thread-safe operations
- Dynamic UDP port allocation

**colors.py** - Presentation Layer
- ANSI escape codes for terminal output

## Configuration

Edit constants in `node.py`:

```python
MULTICAST_GROUP = "224.0.0.1"
MULTICAST_DISCOVERY_PORT = 5007
MULTICAST_PORT_RANGE_START = 5100
MULTICAST_PORT_RANGE_END = 5200
```

Timing parameters (hardcoded in methods):
```python
JOIN_WAIT_TIME = 3          # join_network()
HEARTBEAT_INTERVAL = 2      # _heartbeat_sender()
HEARTBEAT_TIMEOUT = 10      # _heartbeat_monitor()
ELECTION_WAIT_TIME = 3      # start_election()
```

## Testing Scenarios

### Basic Functionality
1. **Single Node**: Becomes coordinator with ID=1, peers list contains itself
2. **Multiple Joins**: Each gets unique ID, all visible in `/peers` including yourself
3. **Message History**: New nodes receive complete history
4. **Invalid Commands**: Commands starting with `/` that don't exist show warning

### Fault Tolerance
5. **Coordinator Failure**: Election starts, highest remaining ID wins
6. **Multiple Failures**: System detects and recovers
7. **Network Partition**: Groups elect coordinators, merge on reconnection

### Edge Cases
8. **Rapid Joins/Leaves**: System remains stable, no conflicts
9. **Port Exhaustion**: 102nd node fails gracefully
10. **Concurrent Elections**: Only one coordinator emerges

## Thread Architecture

Each node runs 5 concurrent daemon threads:

| Thread | Purpose | Interval |
|--------|---------|----------|
| TCP-Listener | Accept TCP connections | Blocking |
| UDP-Listener | Receive UDP multicast | Blocking |
| Discovery-Listener | Receive JOIN_REQUEST | Blocking |
| Heartbeat-Sender | Send health checks | 2 seconds |
| Heartbeat-Monitor | Detect failures | 5 seconds |

### Thread Safety

Protected resources (via `self.lock`):
- `self.peers` - Peer list
- `self.used_multicast_ports` - Allocated ports
- `self.available_udp_ports` - Port pool
- `self.last_heartbeat` - Heartbeat timestamps
- `self.message_history` - Chat history
- `self.election_in_progress` - Election state

## Limitations

1. UDP multicast limited to local network (no internet routing)
2. Each node requires unique TCP port
3. Maximum 101 concurrent nodes (UDP port range)
4. UDP packet size limit (~4KB)
5. No clock synchronization between nodes
6. No message encryption or authentication
7. Designed for local testing only

## Distributed Systems Concepts

### Implementation Coverage

| Concept | Implementation |
|---------|----------------|
| Decentralization | No single point of failure; transferable coordinator role |
| Service Discovery | UDP multicast for peer detection |
| Leader Election | Bully algorithm for coordinator selection |
| Failure Detection | Heartbeat-based monitoring |
| State Replication | Synchronized message history |
| Resource Allocation | Centralized UDP port pool management |
| Concurrency | Thread-safe operations with locks |

### Networking Concepts

- TCP vs UDP tradeoffs (reliability vs speed)
- Multicast for one-to-many communication
- Socket programming (bind, listen, connect)
- Dynamic port allocation and management

### Concurrency Concepts

- Multithreading for I/O operations
- Lock-based synchronization
- Daemon threads for background tasks
- Asynchronous event callbacks

## Troubleshooting

**Port already in use**
- Choose different TCP port
- Check with: `lsof -i :<port>` (Linux/Mac) or `netstat -ano | findstr :<port>` (Windows)

**No coordinator found**
- Verify firewall allows UDP multicast (224.0.0.1)
- Ensure all nodes on same subnet
- Wait full 3 seconds after starting first node

**Messages not appearing**
- Verify nodes visible in `/peers`
- Check TCP and UDP ports not blocked
- Review console for error messages

**Election failures**
- Check network connectivity between nodes
- Verify heartbeats being sent
- Ensure no firewall blocking TCP connections

## Implementation Details

### Peer Management

Each node maintains a `self.peers` dictionary that:
- **Coordinator**: Contains all nodes including itself (for consistent counting)
- **Non-Coordinator**: Receives full peer list from coordinator including itself
- **Format**: `{node_id: (ip, tcp_port, udp_port, name)}`

This ensures:
- Consistent node count across all nodes (`len(node.peers)`)
- Easy iteration for broadcasting messages
- Simplified peer list display in `/peers` command

### Network Communication Simplifications

The system uses only **2 send methods**:
- `_send_to(message, ip, port)` - Send TCP message to specific address
- `_broadcast(message)` - Iterate through all peers and call `_send_to` for each

Removed unnecessary wrapper method `_send_to_peer` for cleaner code.

### UDP Port Allocation

The coordinator maintains a **centralized port pool**:
```python
self.available_udp_ports = set(range(5100, 5201))  # 101 ports
```

When a node joins:
1. If requested port is available → assign it
2. If not available → pop from available pool
3. Add to `used_multicast_ports`
4. Remove from `available_udp_ports`

When a node leaves/fails:
1. Return port to `available_udp_ports`
2. Remove from `used_multicast_ports`

## Command Examples

### `/peers` Output
```
┌─── Peer List ────────────────────┐
│ Connected:
│  • Lucas
│  • Julia
│  • Maria
│
│ Total nodes: 3
└──────────────────────────────────┘
```

### `/status` Output
```
┌─── Node Status ──────────────────┐
│ Name: Lucas
│ ID: #1
│ Address: 127.0.0.1:5001
│ UDP Port: 5150
│ Coordinator: Lucas
│ Active peers: 2
└──────────────────────────────────┘
```

### `/history` Output
```
┌─── Message History ──────────────┐
│ [14:23:45] Lucas: Hello!
│ [14:23:50] Julia: Hi Lucas!
│ [14:24:01] Lucas: How are you?
└──────────────────────────────────┘
```

**Note:** Your own messages appear in green, others in purple.