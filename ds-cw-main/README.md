# Distributed Prime Finder

A distributed application that finds unique prime numbers from large datasets using RPC-based Coordinator-Worker architecture with Primary-Backup replicated AFS.

## Architecture

```
Coordinator :50054  →  Workers (gRPC Clients)
     ↓
AFS Cluster (Primary-Backup Replication)
  - Primary :50051 (Leader)
  - Backup  :50052, :50053 (Replicas)
```

**Features:**
- Primary-Backup replication with automatic failover
- Streaming RPC for efficient file transfer
- Automatic primary election and client discovery
- Fault tolerance with snapshot recovery

## Quick Start

### Build

```bash
# Build AFS server
cd Basic_Operation && make clean && make

# Build prime finder
cd ../prime_finder && make clean && make

# Setup test data
make setup && make setup_afs
```

### Run

**Terminal 1-3: Start AFS Servers**
```bash
cd Basic_Operation
./afs_server 50051 0.0.0.0:50052 0.0.0.0:50053  # Server 1
./afs_server 50052 0.0.0.0:50051 0.0.0.0:50053  # Server 2
./afs_server 50053 0.0.0.0:50051 0.0.0.0:50052  # Server 3
```

**Terminal 4: Start Coordinator**
```bash
cd prime_finder
./coordinator 50054 "localhost:50051,localhost:50052,localhost:50053" test_data run
```

**Terminal 5: Start Workers**
```bash
cd prime_finder
./start_workers.sh localhost:50054 4
```

Or start workers manually:
```bash
./worker localhost:50054  # Worker 1
./worker localhost:50054  # Worker 2
./worker localhost:50054  # Worker 3
./worker localhost:50054  # Worker 4
```

### View Results

```bash
cat /tmp/output/primes_output.txt
wc -l /tmp/output/primes_output.txt
```

## Fault Tolerance

### Primary Failure
- Kill PRIMARY server → Remaining servers elect new PRIMARY
- Clients automatically reconnect to new PRIMARY
- Work continues without interruption

### Snapshot Recovery
```bash
# Restart coordinator with snapshot recovery
./coordinator 50054 "localhost:50051,localhost:50052,localhost:50053" test_data restart
```

## Configuration

### Test Data Size
Edit `prime_finder/Makefile`:
```makefile
shuf -i 1-10000000 -n 2000000 > test_data/input_large_01.txt
```

### Task Chunk Size
Edit `CHUNK_SIZE` in `prime_finder/coordinator.cpp`:
```cpp
#define CHUNK_SIZE 10000  // Lines per task
```

## Protocol

### AFS (Client-Server)
- `open()` / `close()` / `compare()`: File operations (streaming RPC)
- `GetStatus()`: Primary discovery
- `Heartbeat()` / `ReplicateWrite()` / `RequestVote()`: Internal replication

### Coordinator-Worker
- `RegisterWorker()`: Worker registration
- `RequestTask()`: Task assignment
- `SubmitResult()`: Result submission
- `sendHeartbeat()`: Worker liveness check
- `sendMarker()`: Snapshot marker

## Replication

- **Primary**: Handles client requests, replicates writes to backups
- **Backup**: Receives replicated writes, participates in election
- **Election**: Raft-like consensus (majority vote required)
- **Quorum**: N/2 + 1 servers required for election and write confirmation

## Cleanup

```bash
./cleanup.sh  # Kills processes and cleans build artifacts
```
