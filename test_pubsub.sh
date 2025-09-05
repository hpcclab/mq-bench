#!/bin/bash

# Zenoh Publisher/Subscriber Test Script
# This script starts a subscriber in the background, then runs a publisher

set -e

# Ensure logs directory exists
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# Configuration
ENDPOINT="tcp/127.0.0.1:7447"
TOPIC="bench/topic"
PAYLOAD_SIZE=200
RATE=5
DURATION=15

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Zenoh Publisher/Subscriber Test ===${NC}"
echo "Configuration:"
echo "  Endpoint: $ENDPOINT"
echo "  Topic: $TOPIC"
echo "  Payload size: $PAYLOAD_SIZE bytes"
echo "  Rate: $RATE msg/s"
echo "  Duration: $DURATION seconds"
echo

# Build the harness first
echo -e "${YELLOW}Building mq-bench...${NC}"
cargo build --quiet
if [ $? -ne 0 ]; then
    echo -e "${RED}Build failed!${NC}"
    exit 1
fi
echo -e "${GREEN}Build successful${NC}"
echo

# Check if Docker cluster is running
echo -e "${YELLOW}Checking Docker cluster...${NC}"
docker compose ps | grep -q "Up"
if [ $? -ne 0 ]; then
    echo -e "${RED}Docker cluster not running! Starting it...${NC}"
    docker compose up -d
    sleep 5
else
    echo -e "${GREEN}Docker cluster is running${NC}"
fi
echo

# Kill any existing processes
echo -e "${YELLOW}Cleaning up any existing test processes...${NC}"
pkill -f "mq-bench sub" || true
pkill -f "mq-bench pub" || true
sleep 2

# Create output files with timestamps
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
SUB_LOG="$LOG_DIR/subscriber_${TIMESTAMP}.log"
PUB_LOG="$LOG_DIR/publisher_${TIMESTAMP}.log"
SUB_CSV="$LOG_DIR/subscriber_${TIMESTAMP}.csv"

echo -e "${YELLOW}Starting subscriber in background...${NC}"
# Start subscriber in background, redirecting output to log file
./target/debug/mq-bench sub \
    --endpoint "$ENDPOINT" \
    --expr "$TOPIC" \
    > "$SUB_LOG" 2>&1 &

SUB_PID=$!
echo "Subscriber PID: $SUB_PID"

# Wait a moment for subscriber to initialize
sleep 3

# Check if subscriber is still running
if ! kill -0 $SUB_PID 2>/dev/null; then
    echo -e "${RED}Subscriber failed to start!${NC}"
    cat "$SUB_LOG"
    exit 1
fi

echo -e "${GREEN}Subscriber started successfully${NC}"
echo "Subscriber log: $SUB_LOG"
echo

# Start publisher in foreground
echo -e "${YELLOW}Starting publisher...${NC}"
echo "This will run for $DURATION seconds..."
echo

./target/debug/mq-bench pub \
    --endpoint "$ENDPOINT" \
    --payload "$PAYLOAD_SIZE" \
    --rate "$RATE" \
    --duration "$DURATION" \
    --topic-prefix "$TOPIC" \
    2>&1 | tee "$PUB_LOG"

PUB_EXIT_CODE=$?

echo
echo -e "${YELLOW}Publisher completed with exit code: $PUB_EXIT_CODE${NC}"

# Give subscriber a moment to process final messages
sleep 2

# Stop subscriber gracefully
echo -e "${YELLOW}Stopping subscriber...${NC}"
kill -TERM $SUB_PID 2>/dev/null || true

# Wait for subscriber to finish
sleep 3

# Force kill if still running
if kill -0 $SUB_PID 2>/dev/null; then
    echo "Force killing subscriber..."
    kill -KILL $SUB_PID 2>/dev/null || true
fi

echo -e "${GREEN}Test completed!${NC}"
echo

# Show final results
echo -e "${BLUE}=== Test Results ===${NC}"
echo

echo -e "${YELLOW}Publisher Results:${NC}"
if [ -f "$PUB_LOG" ]; then
    echo "Final statistics from publisher:"
    grep -A 10 "Final Publisher Statistics:" "$PUB_LOG" || echo "No final statistics found"
else
    echo "No publisher log found"
fi
echo

echo -e "${YELLOW}Subscriber Results:${NC}"
if [ -f "$SUB_LOG" ]; then
    echo "Last few lines from subscriber:"
    tail -20 "$SUB_LOG"
    echo
    echo "Looking for received message count:"
    grep -E "(Received:|Messages received:|Subscriber stats)" "$SUB_LOG" | tail -5 || echo "No receive stats found"
else
    echo "No subscriber log found"
fi
echo

# Show log file locations
echo -e "${BLUE}Log Files:${NC}"
echo "Publisher log: $PUB_LOG"
echo "Subscriber log: $SUB_LOG"
if [ -f "$SUB_CSV" ]; then
    echo "Subscriber CSV: $SUB_CSV"
fi

# Cleanup function
cleanup() {
    echo
    echo -e "${YELLOW}Cleaning up background processes...${NC}"
    pkill -f "mq-bench" || true
}

# Set trap for cleanup on script exit
trap cleanup EXIT

echo
echo -e "${GREEN}Test script finished!${NC}"
