# Smart Traffic Control System

A sophisticated distributed traffic management simulation implementing advanced distributed systems concepts including microservices architecture, load balancing, mutual exclusion, clock synchronization, and fault-tolerant data storage.

## System Architecture

This system demonstrates a real-world distributed traffic management platform with the following key characteristics:

- **Microservices Pattern**: Independent, specialized services communicating via XML-RPC
- **Fault Tolerance**: No single point of failure through distributed responsibilities
- **Scalability**: Dynamic load balancing and horizontal scaling support
- **Data Consistency**: GFS-inspired distributed storage with consistency guarantees

## Core Components

### Data Layer Services
- **`master_server.py`** - Central coordinator managing metadata for data chunks (inspired by Google File System)
- **`replica_server.py`** - Data storage nodes holding partitioned state data (`replica_1.db`, `replica_2.db`, etc.)

### Coordination Services
- **`zookeeper.py`** - Multi-purpose coordination service providing:
  - Service discovery for clients
  - Dynamic load balancing
  - Clock synchronization (Berkeley Algorithm)
  - Deadlock detection and resolution

### Traffic Control Services
- **`controller.py`** & **`controller_clone.py`** - Core traffic management logic with mutual exclusion
- **`p_signal.py`** & **`t_signal.py`** - Pedestrian and traffic signal clients
- **`proxy_server.py`** - Gateway between web frontend and distributed backend
- **`rto_client.py`** - External client simulating Regional Transport Office interactions

### Frontend
- **`Frontend/traffic_monitor.html`** - Real-time interactive dashboard for intersection visualization

## Key Distributed Systems Implementations

### 1. Load Balancing with ZooKeeper
- **Dynamic Routing**: Automatic request distribution to least loaded controllers
- **Scalability**: Seamless addition of new controller instances
- **Service Discovery**: Centralized coordination without tight coupling

### 2. Mutual Exclusion (Ricart-Agrawala Algorithm)
- **Critical Section Protection**: Safe signal state changes
- **Timestamp-based Priority**: Lamport logical clocks for request ordering
- **Distributed Consensus**: All controllers must approve before signal changes
- **Dual Acknowledgement**: Additional pedestrian signal safety checks

### 3. Clock Synchronization (Berkeley Algorithm)
- **Time Master**: ZooKeeper maintains system-wide time consistency
- **Periodic Sync**: Regular time polling and offset corrections
- **Essential for Mutual Exclusion**: Ensures timestamp reliability

### 4. Distributed Storage (GFS-like Model)
- **Master-Replica Architecture**: Metadata management with distributed data storage
- **Reader-Writer Locks**: High-concurrency reads with exclusive writes
- **Data Consistency**: Guaranteed consistency during concurrent access

## Getting Started

### Prerequisites
- Python 3.x installed
- Required packages: `requests` and other standard library modules

```bash
pip install requests
```

### Installation & Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd smart-traffic-control-system
```

### Running the System Locally

The following instructions are for running all services on your local machine. For distributed deployment across multiple machines, replace `localhost` or `127.0.0.1` with the appropriate IP addresses of your target machines in the respective configuration files.

**Important Notes for Multi-Machine Deployment:**
- Ensure all machines can communicate on the specified ports
- On Windows systems, you may need to enable the RPC service if it's not running
- Configure firewall inbound rules to allow traffic on the ports used by each service
- Update any hardcoded IP addresses in the Python files to match your network topology

**Start all services** (each in a separate terminal from the project root):

#### Terminal 1 - Master Server
```bash
python master_server.py
```

#### Terminals 2-4 - Replica Servers
```bash
# Terminal 2
python replica_server.py 7001 1

# Terminal 3
python replica_server.py 7002 2

# Terminal 4
python replica_server.py 7003 3
```

#### Terminal 5 - ZooKeeper Coordination Service
```bash
python zookeeper.py
```

#### Terminals 6-7 - Traffic Controllers
```bash
# Terminal 6 - Main Controller
python controller.py

# Terminal 7 - Controller Clone
python controller_clone.py
```

#### Terminals 8-9 - Signal Devices
```bash
# Terminal 8 - Pedestrian Signals
python p_signal.py

# Terminal 9 - Traffic Signals
python t_signal.py
```

#### Terminal 10 - Proxy Server
```bash
python proxy_server.py
```

#### Terminal 11 - Frontend Web Server
```bash
cd Frontend
python -m http.server 3000
```

2. **Access the System**
   
   Open your web browser and navigate to:
   ```
   http://localhost:3000/traffic_monitor.html
   ```

## System Features

### Real-time Traffic Management
- Dynamic signal control based on traffic conditions
- Pedestrian crossing integration
- VIP vehicle priority handling

### High Availability
- Multiple controller instances for redundancy
- Automatic failover and load distribution
- Fault-tolerant data storage across replicas

### Monitoring & Visualization
- Live dashboard showing intersection status
- Manual traffic request triggering
- System performance metrics

### External Integration
- RTO client for administrative operations
- RESTful proxy interface for external systems
- Configurable priority levels and policies

## Technical Highlights

This project demonstrates practical implementations of:

- **Distributed Mutual Exclusion** for safe resource access
- **Leader Election** and coordination patterns
- **Clock Synchronization** in distributed environments
- **Consistent Hashing** for data distribution
- **Fault Detection** and recovery mechanisms
- **Load Balancing** algorithms
- **Deadlock Prevention** in concurrent systems

## Development & Testing

The system supports:
- Horizontal scaling by adding more controller clones
- Configuration changes without system restart
- Comprehensive logging for debugging and monitoring
- Graceful degradation under component failures

## License

Free to use for any purpose.

---

**Note**: This is a simulation system designed for educational purposes to demonstrate distributed systems concepts. It is not intended for production traffic management use.