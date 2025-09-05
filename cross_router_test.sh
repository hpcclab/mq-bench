#!/bin/bash
set -e

echo "=== Testing with Different Routers ==="

# Ensure logs directory exists
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# Kill any existing processes
pkill -f mq-bench || true
sleep 1

# Start subscriber on router2 (port 7448)
echo "Starting subscriber on router2..."
./target/debug/mq-bench sub --endpoint tcp/127.0.0.1:7448 --expr "cross/test" > "$LOG_DIR/debug_sub_r2.log" 2>&1 &
SUB_PID=$!

# Wait for subscriber to connect
sleep 3
echo "Subscriber connected to router2, starting publisher on router3..."

# Start publisher on router3 (port 7449) 
echo "Starting publisher on router3..."
./target/debug/mq-bench pub --endpoint tcp/127.0.0.1:7449 --topic-prefix "cross/test" --payload 100 --rate 1 --duration 5 > "$LOG_DIR/debug_pub_r3.log" 2>&1 &
PUB_PID=$!

# Wait for test to complete
sleep 8

# Kill processes
if kill -0 $PUB_PID 2>/dev/null; then kill $PUB_PID; fi
if kill -0 $SUB_PID 2>/dev/null; then kill $SUB_PID; fi
wait 2>/dev/null || true

echo ""
echo "=== Publisher (Router3) Log ==="
cat "$LOG_DIR/debug_pub_r3.log"
echo ""
echo "=== Subscriber (Router2) Log ==="  
cat "$LOG_DIR/debug_sub_r2.log"
echo ""
echo "=== Cross-Router Test Complete ==="
