#!/bin/bash
# Start multiple workers from a single terminal
# Usage: ./start_workers.sh <coordinator_address> <num_workers>

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <coordinator_address> <num_workers>"
    echo "Example: $0 localhost:50052 4"
    exit 1
fi

COORDINATOR_ADDRESS=$1
NUM_WORKERS=$2

echo "=== Starting $NUM_WORKERS Workers ==="
echo "Coordinator: $COORDINATOR_ADDRESS"
echo ""

for i in $(seq 1 $NUM_WORKERS); do
    echo "Starting worker $i..."
    ./worker $COORDINATOR_ADDRESS > worker_${i}.log 2>&1 &
    sleep 0.2  # Small delay between workers
done

echo ""
echo "✓ Started $NUM_WORKERS workers in background"
echo "Logs: worker_1.log, worker_2.log, ..."
echo ""
echo "To view logs:"
echo "  tail -f worker_*.log"
echo ""
echo "To stop all workers:"
echo "  ./cleanup.sh"
