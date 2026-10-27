#!/bin/bash

echo "=== Cleanup Script ==="
echo ""

# Kill all running processes
echo "Step 1: Killing processes..."

# Kill AFS server
AFS_PIDS=$(ps aux | grep afs_server | grep -v grep | awk '{print $2}')
if [ ! -z "$AFS_PIDS" ]; then
    kill -9 $AFS_PIDS 2>/dev/null
    echo "  ✓ Killed AFS server"
fi

# Kill coordinator
COORD_PIDS=$(ps aux | grep "./coordinator" | grep -v grep | awk '{print $2}')
if [ ! -z "$COORD_PIDS" ]; then
    kill -9 $COORD_PIDS 2>/dev/null
    echo "  ✓ Killed coordinator"
fi

# Kill workers
WORKER_PIDS=$(ps aux | grep "./worker" | grep -v grep | awk '{print $2}')
if [ ! -z "$WORKER_PIDS" ]; then
    COUNT=$(echo "$WORKER_PIDS" | wc -w)
    kill -9 $WORKER_PIDS 2>/dev/null
    echo "  ✓ Killed $COUNT workers"
fi

sleep 1

# Clean build artifacts
echo ""
echo "Step 2: Cleaning build artifacts..."

# Clean prime_finder
cd /usr/src/ds-cw/prime_finder
make clean > /dev/null 2>&1
echo "  ✓ Cleaned prime_finder/"

# Clean Basic_Operation
cd /usr/src/ds-cw/Basic_Operation
make clean > /dev/null 2>&1
echo "  ✓ Cleaned Basic_Operation/"

# Clean output files
echo ""
echo "Step 3: Cleaning output files..."
rm -rf /tmp/output/* /tmp/input/* 2>/dev/null
rm -f /usr/src/ds-cw/prime_finder/worker_*.log 2>/dev/null
rm -f /usr/src/ds-cw/prime_finder/coordinator.log 2>/dev/null
echo "  ✓ Cleaned /tmp/output/ and /tmp/input/"

echo ""
echo "✓ Cleanup complete!"
