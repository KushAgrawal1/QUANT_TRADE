#!/bin/bash
echo "Starting all services..."

PIDS=()

cleanup() {
    echo ""
    echo "Stopping all services..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    echo "All services stopped."
    exit 0
}
trap cleanup INT TERM

# Start producer in background
python producer.py &
PIDS+=($!)
echo "Producer started (PID ${PIDS[-1]})"

# Start stock producer in background
python stock_producer.py &
PIDS+=($!)
echo "Stock producer started (PID ${PIDS[-1]})"

# Wait 3 seconds then start trader
sleep 3
python trader.py &
PIDS+=($!)
echo "Trader started (PID ${PIDS[-1]})"

echo "All services running. Press Ctrl+C to stop all."

# Wait on all children; Ctrl+C triggers cleanup() which kills them all
wait