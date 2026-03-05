#!/usr/bin/env bash
#
# Test QoS 2 (exactly-once) delivery WITHOUT crash injection.
# This establishes a baseline for QoS 2 correctness under normal conditions.
#
# The purpose of this test is to verify:
# 1. QoS 2 delivers exactly-once (no duplicates, no loss)
# 2. Broker handles QoS 2 transactions correctly
# 3. The benchmark harness correctly measures QoS 2 performance
#
# Note: QoS 2 with crash injection has fundamental limitations because
# rumqttc doesn't persist transaction state across process restarts.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="${PROJECT_DIR}/target/release/mq-bench"
ARTIFACTS_DIR="${PROJECT_DIR}/artifacts/qos2_baseline_$(date +%Y%m%d_%H%M%S)"

# Test parameters
BROKER_HOST="${BROKER_HOST:-127.0.0.1}"
BROKER_PORT="${BROKER_PORT:-1883}"
DURATION=30
RATES=(100 500 1000 2000)
PAYLOAD=128

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_test() { echo -e "${BLUE}[TEST]${NC} $*"; }

cleanup() {
    log_info "Cleaning up..."
    jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$ARTIFACTS_DIR"

check_prerequisites() {
    if [[ ! -f "$BINARY" ]]; then
        log_error "Binary not found. Run: cargo build --release"
        exit 1
    fi

    if ! nc -z "$BROKER_HOST" "$BROKER_PORT" 2>/dev/null; then
        log_error "MQTT broker not running on $BROKER_HOST:$BROKER_PORT"
        log_info "Start with: docker compose up -d mosquitto"
        exit 1
    fi
    log_info "Prerequisites OK"
}

run_qos_test() {
    local qos="$1"
    local rate="$2"
    local test_id="qos${qos}_rate${rate}"
    local topic="qos2_test/${test_id}"
    local sub_csv="$ARTIFACTS_DIR/${test_id}_sub.csv"
    local pub_log="$ARTIFACTS_DIR/${test_id}_pub.log"
    local sub_log="$ARTIFACTS_DIR/${test_id}_sub.log"

    log_test "Testing QoS ${qos} at ${rate} msg/s for ${DURATION}s..."

    # Start subscriber (with unique client_id for persistent session)
    "$BINARY" sub \
        --engine mqtt \
        --connect "host=$BROKER_HOST" \
        --connect "port=$BROKER_PORT" \
        --connect "qos=$qos" \
        --connect "client_id=sub-${test_id}" \
        --connect "clean_session=false" \
        --expr "$topic" \
        --csv "$sub_csv" \
        >"$sub_log" 2>&1 &
    local sub_pid=$!
    
    # Wait for subscriber to connect
    sleep 2

    # Start publisher
    "$BINARY" pub \
        --engine mqtt \
        --connect "host=$BROKER_HOST" \
        --connect "port=$BROKER_PORT" \
        --connect "qos=$qos" \
        --connect "client_id=pub-${test_id}" \
        --connect "clean_session=false" \
        --topic-prefix "$topic" \
        --payload "$PAYLOAD" \
        --rate "$rate" \
        --duration "$DURATION" \
        >"$pub_log" 2>&1

    # Wait for subscriber to drain messages
    sleep 8
    
    # Graceful shutdown
    kill -INT $sub_pid 2>/dev/null || true
    for i in {1..20}; do
        if ! kill -0 $sub_pid 2>/dev/null; then break; fi
        sleep 0.5
    done
    kill -9 $sub_pid 2>/dev/null || true
    wait $sub_pid 2>/dev/null || true

    # Parse results
    local sent=0 received=0 duplicates=0 gaps=0
    
    if grep -q "Final Publisher Statistics" "$pub_log"; then
        sent=$(grep "Final Publisher Statistics" "$pub_log" | sed 's/.*sent=\([0-9]*\).*/\1/')
    fi
    
    if grep -q "Final Subscriber Statistics" "$sub_log"; then
        received=$(grep "Final Subscriber Statistics" "$sub_log" | sed 's/.*received=\([0-9]*\).*/\1/')
        duplicates=$(grep "Final Subscriber Statistics" "$sub_log" | sed 's/.*duplicates=\([0-9]*\).*/\1/')
        gaps=$(grep "Final Subscriber Statistics" "$sub_log" | sed 's/.*gaps=\([0-9]*\).*/\1/')
    fi
    
    local loss=0
    if [[ "$sent" -gt 0 ]]; then
        loss=$(echo "scale=2; 100 * ($sent - $received) / $sent" | bc)
    fi

    # Determine pass/fail
    local status="PASS"
    local status_color="$GREEN"
    
    if [[ "$qos" -eq 2 ]]; then
        # QoS 2 should have exactly-once: no loss, no duplicates
        if [[ "$gaps" -ne 0 ]] || [[ "$duplicates" -ne 0 ]] || [[ "$received" -ne "$sent" ]]; then
            status="FAIL"
            status_color="$RED"
        fi
    elif [[ "$qos" -eq 1 ]]; then
        # QoS 1: no loss allowed, duplicates are acceptable
        if [[ "$gaps" -ne 0 ]] || [[ "$received" -lt "$sent" ]]; then
            status="FAIL"
            status_color="$RED"
        fi
    fi
    
    echo -e "${status_color}[${status}]${NC} QoS=$qos Rate=$rate: sent=$sent received=$received loss=${loss}% duplicates=$duplicates gaps=$gaps"
    
    # Return result for summary
    echo "$qos,$rate,$sent,$received,$loss,$duplicates,$gaps,$status" >> "$ARTIFACTS_DIR/summary.csv"
}

# Main
log_info "========================================="
log_info "QoS 2 Baseline Test (No Crash Injection)"
log_info "========================================="
log_info "Artifacts: $ARTIFACTS_DIR"

check_prerequisites

# Write CSV header
echo "qos,rate,sent,received,loss_pct,duplicates,gaps,status" > "$ARTIFACTS_DIR/summary.csv"

# Test QoS 1 and QoS 2 at various rates
for qos in 1 2; do
    log_info ""
    log_info "===== Testing QoS $qos ====="
    for rate in "${RATES[@]}"; do
        run_qos_test "$qos" "$rate"
        # Small pause between tests to let broker settle
        sleep 2
    done
done

log_info ""
log_info "========================================="
log_info "SUMMARY"
log_info "========================================="
echo ""
column -t -s',' "$ARTIFACTS_DIR/summary.csv"
echo ""

# Count failures
failures=$(grep -c ",FAIL$" "$ARTIFACTS_DIR/summary.csv" || true)
passes=$(grep -c ",PASS$" "$ARTIFACTS_DIR/summary.csv" || true)

if [[ "$failures" -eq 0 ]]; then
    log_info "${GREEN}All tests passed!${NC} ($passes tests)"
    exit 0
else
    log_error "${RED}$failures tests failed${NC} ($passes passed)"
    exit 1
fi
