#!/bin/bash
echo "Starting all services..."

# Start producer in background
python producer.py &
PRODUCER_PID=$!
echo "Producer started (PID $PRODUCER_PID)"

# Start stock producer in background  
python stock_producer.py &
STOCK_PID=$!
echo "Stock producer started (PID $STOCK_PID)"

# Wait 3 seconds then start trader
sleep 3
python trader.py &
TRADER_PID=$!
echo "Trader started (PID $TRADER_PID)"

echo "All services running. Press Ctrl+C to stop all."

# If any process dies, kill everything and restart
wait
