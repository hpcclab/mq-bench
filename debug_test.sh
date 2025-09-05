#!/bin/bash
set -e

echo "=== Debug Test: Simple Pub/Sub with same key ==="

# Ensure logs directory exists
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# Kill any existing processes
pkill -f mq-bench || true
sleep 1

# Start subscriber with debug output
echo "Starting subscriber..."
./target/debug/mq-bench sub --endpoint tcp/127.0.0.1:7447 --expr "debug/test" > "$LOG_DIR/debug_sub.log" 2>&1 &
SUB_PID=$!

# Wait for subscriber to fully connect
sleep 3
echo "Subscriber started (PID: $SUB_PID), waiting 3 seconds for connection..."

# Start publisher for just 5 messages
echo "Starting publisher to send 5 messages..."
./target/debug/mq-bench pub --endpoint tcp/127.0.0.1:7447 --topic-prefix "debug/test" --payload 100 --rate 2 --duration 3 > "$LOG_DIR/debug_pub.log" 2>&1 &
PUB_PID=$!

# Wait for publisher to finish
sleep 5

# Check if processes are still running
if kill -0 $PUB_PID 2>/dev/null; then
    echo "Killing publisher..."
    kill $PUB_PID
fi

if kill -0 $SUB_PID 2>/dev/null; then
    echo "Killing subscriber..."
    kill $SUB_PID
fi

wait 2>/dev/null || true

echo ""
echo "=== Publisher Log ==="
cat "$LOG_DIR/debug_pub.log"
echo ""
echo "=== Subscriber Log ==="
cat "$LOG_DIR/debug_sub.log"
echo ""
echo "=== Test Complete ==="
