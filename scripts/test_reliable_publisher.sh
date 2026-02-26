#!/usr/bin/env bash
#
# End-to-end test for Reliable Publisher with sequence recovery.
#
# This test verifies that:
# - Reliable publisher waits for ACK before confirming messages
# - On crash, publisher resumes from last confirmed sequence
# - Subscriber can crash and recover with persistent sessions
# - Messages are not lost even when both sides crash
#
# Tests both QoS 1 (at-least-once) and QoS 2 (exactly-once)
# Crashes happen on BOTH publisher AND subscriber sides.
#
# Prerequisites:
#   - Mosquitto broker running on localhost:1883
#   - cargo build --release

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="${PROJECT_DIR}/target/release/mq-bench"
ARTIFACTS_DIR="${PROJECT_DIR}/artifacts/reliable_test_$(date +%Y%m%d_%H%M%S)"

# Test parameters
BROKER_HOST="${BROKER_HOST:-127.0.0.1}"
BROKER_PORT="${BROKER_PORT:-1883}"
RATE="${RATE:-1000}"
PUB_MTTF="${PUB_MTTF:-1.5}"     # Publisher mean time to failure (seconds) - shorter = more crashes
PUB_MTTR="${PUB_MTTR:-1}"       # Publisher mean time to recovery (seconds)
SUB_MTTF="${SUB_MTTF:-1.5}"     # Subscriber mean time to failure (seconds)
SUB_MTTR="${SUB_MTTR:-1}"       # Subscriber mean time to recovery (seconds)
CRASH_COUNT="${CRASH_COUNT:-8}"  # Max crashes per side
DURATION="${DURATION:-20}"       # Longer duration to allow for crashes on both sides

# Drain behavior after publisher exits
# - QoS 2 + persistent sessions + hard crashes can wedge on an incomplete PUBREL/PUBCOMP.
#   When this happens, Mosquitto may keep retrying the same PUBREL and stop delivering newer queued messages.
DRAIN_TIMEOUT_SECS="${DRAIN_TIMEOUT_SECS:-90}"
DRAIN_IDLE_SECS="${DRAIN_IDLE_SECS:-5}"

# To make crash injection more comparable across back-to-back tests, keep the subscriber alive for a fixed
# wall-clock duration per test case. Otherwise, different drain times change how many crashes occur even
# with the same seed.
SUB_TOTAL_SECS="${SUB_TOTAL_SECS:-$((DURATION + 40))}"

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
log_section() { echo -e "${CYAN}[====]${NC} $*"; }

cleanup() {
    log_info "Cleaning up..."
    jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT

csv_last_field() {
    local csv_file="$1"
    local field_idx="$2"
    if [[ ! -f "$csv_file" ]]; then
        echo "0"
        return 0
    fi
    tail -n 1 "$csv_file" | cut -d',' -f"$field_idx" 2>/dev/null || echo "0"
}

wait_for_subscriber_drain() {
    local sub_pid="$1"
    local sub_csv="$2"
    local expected="$3"  # 0 disables waiting for expected
    local hard_deadline_ts="${4:-0}" # 0 disables

    local start_ts
    start_ts=$(date +%s)
    local last_progress_ts="$start_ts"
    local last_received="-1"

    while true; do
        # Stop if subscriber already exited
        if ! kill -0 "$sub_pid" 2>/dev/null; then
            break
        fi

        local now_ts
        now_ts=$(date +%s)
        local elapsed=$((now_ts - start_ts))
        if [[ "$elapsed" -ge "$DRAIN_TIMEOUT_SECS" ]]; then
            break
        fi

        if [[ "$hard_deadline_ts" -gt 0 && "$now_ts" -ge "$hard_deadline_ts" ]]; then
            break
        fi

        local received
        received=$(csv_last_field "$sub_csv" 3)

        if [[ "$received" != "$last_received" ]]; then
            last_received="$received"
            last_progress_ts="$now_ts"
        fi

        if [[ "$expected" -gt 0 && "$received" -ge "$expected" ]]; then
            break
        fi

        local idle=$((now_ts - last_progress_ts))
        if [[ "$idle" -ge "$DRAIN_IDLE_SECS" ]]; then
            break
        fi

        sleep 1
    done
}

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

# Run a single test case with crashes on both sides
# Arguments: $1=test_name, $2=qos, $3=publisher_type (pub|rel-pub), $4=test_id, $5=sub_crashes (true|false)
run_single_test() {
    local test_name="$1"
    local qos="$2"
    local pub_type="$3"
    local test_id="$4"
    local sub_crashes="${5:-false}"
    
    local topic="reliable_test/${test_id}/${test_name}"
    local sub_csv="$ARTIFACTS_DIR/${test_name}_sub.csv"
    local pub_log="$ARTIFACTS_DIR/${test_name}_pub.log"
    local sub_log="$ARTIFACTS_DIR/${test_name}_sub.log"

    log_test "--- ${test_name} (QoS ${qos}, sub_crashes=${sub_crashes}) ---"

    # Build subscriber command
    local sub_cmd=(
        "$BINARY" sub
        --engine mqtt
        --connect "host=$BROKER_HOST"
        --connect "port=$BROKER_PORT"
        --connect "qos=$qos"
        --connect "client_id=sub-${test_name}-${test_id}"
        --connect "clean_session=false"
        --expr "$topic"
        --csv "$sub_csv"
        --enable-retry
    )
    
    # Add crash injection for subscriber if enabled
    if [[ "$sub_crashes" == "true" ]]; then
        sub_cmd+=(
            --mttf "$SUB_MTTF"
            --mttr "$SUB_MTTR"
            --crash-count "$CRASH_COUNT"
            --crash-seed 54321
        )
    fi

    # Start subscriber
    "${sub_cmd[@]}" >"$sub_log" 2>&1 &
    local sub_pid=$!
    local sub_start_ts
    sub_start_ts=$(date +%s)
    local sub_deadline_ts=$((sub_start_ts + SUB_TOTAL_SECS))
    # Wait for subscriber to connect and subscribe before starting publisher
    # This prevents losing early messages before subscription is active
    sleep 2

    # Start publisher with crash injection
    if [[ "$pub_type" == "rel-pub" ]]; then
        "$BINARY" rel-pub \
            --connect "host=$BROKER_HOST" \
            --connect "port=$BROKER_PORT" \
            --connect "qos=$qos" \
            --connect "client_id=pub-${test_name}-${test_id}" \
            --connect "clean_session=false" \
            --topic "$topic" \
            --payload 64 \
            --rate "$RATE" \
            --duration "$DURATION" \
            --mttf "$PUB_MTTF" \
            --mttr "$PUB_MTTR" \
            --crash-count "$CRASH_COUNT" \
            --crash-seed 12345 \
            --enable-retry \
            >"$pub_log" 2>&1
    else
        "$BINARY" pub \
            --engine mqtt \
            --connect "host=$BROKER_HOST" \
            --connect "port=$BROKER_PORT" \
            --connect "qos=$qos" \
            --connect "client_id=pub-${test_name}-${test_id}" \
            --connect "clean_session=false" \
            --topic-prefix "$topic" \
            --payload 64 \
            --rate "$RATE" \
            --duration "$DURATION" \
            --mttf "$PUB_MTTF" \
            --mttr "$PUB_MTTR" \
            --crash-count "$CRASH_COUNT" \
            --crash-seed 12345 \
            --enable-retry \
            >"$pub_log" 2>&1
    fi

    # Give subscriber time to drain any queued messages from the broker.
    # For rel-pub, compare against CONFIRMED (PUBACK/PUBCOMP), not SENT.
    local expected_drain=0
    if [[ -f "$pub_log" ]]; then
        if [[ "$pub_type" == "rel-pub" ]] && grep -q "Final Reliable Publisher Statistics" "$pub_log"; then
            expected_drain=$(grep "Final Reliable Publisher Statistics" "$pub_log" | sed 's/.*confirmed=\([0-9]*\).*/\1/' | head -1)
        elif [[ "$pub_type" != "rel-pub" ]] && grep -q "Final Publisher Statistics" "$pub_log"; then
            expected_drain=$(grep "Final Publisher Statistics" "$pub_log" | sed 's/.*sent=\([0-9]*\).*/\1/' | head -1)
        fi
    fi
    wait_for_subscriber_drain "$sub_pid" "$sub_csv" "$expected_drain" "$sub_deadline_ts"

    # Keep subscriber alive until the fixed deadline to stabilize crash injection.
    # This makes crash counts comparable across test cases with the same seed.
    while kill -0 "$sub_pid" 2>/dev/null; do
        local now_ts
        now_ts=$(date +%s)
        if [[ "$now_ts" -ge "$sub_deadline_ts" ]]; then
            break
        fi
        sleep 1
    done
    # Send SIGINT (Ctrl+C) for graceful shutdown so subscriber can report final stats
    kill -INT $sub_pid 2>/dev/null || true
    # Wait for graceful shutdown with timeout (up to 10 seconds)
    for i in {1..20}; do
        if ! kill -0 $sub_pid 2>/dev/null; then
            break
        fi
        sleep 0.5
    done
    # Force kill if still running
    kill -9 $sub_pid 2>/dev/null || true
    wait $sub_pid 2>/dev/null || true

    # Parse results
    local sent=0 received=0 pub_crashes=0 sub_crashes_count=0 confirmed=0 duplicates=0 gaps=0
    local lat_p50=0 lat_p95=0 lat_p99=0
    
    if [[ "$pub_type" == "rel-pub" ]]; then
        if grep -q "Final Reliable Publisher Statistics" "$pub_log"; then
            sent=$(grep "Final Reliable Publisher Statistics" "$pub_log" | sed 's/.*sent=\([0-9]*\).*/\1/' | head -1)
            confirmed=$(grep "Final Reliable Publisher Statistics" "$pub_log" | sed 's/.*confirmed=\([0-9]*\).*/\1/' | head -1)
            pub_crashes=$(grep "Final Reliable Publisher Statistics" "$pub_log" | sed 's/.*crashes=\([0-9]*\).*/\1/' | head -1)
        fi
    else
        if grep -q "Final Publisher Statistics" "$pub_log"; then
            sent=$(grep "Final Publisher Statistics" "$pub_log" | sed 's/.*sent=\([0-9]*\).*/\1/' | head -1)
            pub_crashes=$(grep "Final Publisher Statistics" "$pub_log" | sed 's/.*crashes=\([0-9]*\).*/\1/' | head -1)
        fi
        confirmed="N/A"
    fi
    
    # Try to get subscriber stats from log file first (more reliable for final values)
    if grep -q "Final Subscriber Statistics" "$sub_log"; then
        received=$(grep "Final Subscriber Statistics" "$sub_log" | sed 's/.*received=\([0-9]*\).*/\1/' | head -1)
        duplicates=$(grep "Final Subscriber Statistics" "$sub_log" | sed 's/.*duplicates=\([0-9]*\).*/\1/' | head -1)
        gaps=$(grep "Final Subscriber Statistics" "$sub_log" | sed 's/.*gaps=\([0-9]*\).*/\1/' | head -1)
    elif [[ -f "$sub_csv" ]]; then
        # Fallback to CSV if log doesn't have final stats
        received=$(tail -1 "$sub_csv" | cut -d',' -f3)
        duplicates=$(tail -1 "$sub_csv" | cut -d',' -f20)
        gaps=$(tail -1 "$sub_csv" | cut -d',' -f21)
    fi

    # Get latencies from CSV (always available there)
    if [[ -f "$sub_csv" ]]; then
        # Latency columns: p50=7, p95=8, p99=9 (nanoseconds)
        lat_p50=$(tail -1 "$sub_csv" | cut -d',' -f7)
        lat_p95=$(tail -1 "$sub_csv" | cut -d',' -f8)
        lat_p99=$(tail -1 "$sub_csv" | cut -d',' -f9)
    fi

    # Count subscriber crashes from log (count "Crash injection triggered" lines)
    if [[ -f "$sub_log" ]]; then
        sub_crashes_count=$(grep -c "Crash injection triggered" "$sub_log" 2>/dev/null || echo "0")
    fi

    # Calculate loss
    local loss_pct="N/A"
    local basis_label="sent"
    local basis_value="$sent"
    if [[ "$pub_type" == "rel-pub" ]]; then
        basis_label="confirmed"
        basis_value="$confirmed"
    fi
    if [[ "$basis_value" != "N/A" && "$basis_value" -gt 0 ]]; then
        loss_pct=$(echo "scale=1; ($basis_value - $received) * 100 / $basis_value" | bc 2>/dev/null || echo "0")
    fi

    # Convert latencies from nanoseconds to milliseconds
    local lat_p50_ms=$(echo "scale=2; $lat_p50 / 1000000" | bc 2>/dev/null || echo "0")
    local lat_p95_ms=$(echo "scale=2; $lat_p95 / 1000000" | bc 2>/dev/null || echo "0")
    local lat_p99_ms=$(echo "scale=2; $lat_p99 / 1000000" | bc 2>/dev/null || echo "0")

    # Print results
    echo "  Sent:         $sent"
    [[ "$confirmed" != "N/A" ]] && echo "  Confirmed:    $confirmed"
    [[ "$confirmed" != "N/A" ]] && echo "  Unconfirmed:  $((sent - confirmed))"
    echo "  Received:     $received"
    echo "  Duplicates:   $duplicates"
    echo "  Gaps:         $gaps"
    echo "  Pub Crashes:  $pub_crashes"
    echo "  Sub Crashes:  $sub_crashes_count"
    echo "  Loss (${basis_label}): ${loss_pct}%"
    if [[ "$confirmed" != "N/A" && "$confirmed" -gt "$received" ]]; then
        echo "  Tail Missing: $((confirmed - received))"
    fi
    if [[ -f "$sub_log" ]] && grep -q "Unsolicited pubrel packet" "$sub_log"; then
        echo "  QoS2 State:   WARNING (unsolicited PUBREL seen; session may be wedged)"
    fi
    echo "  Latency p50:  ${lat_p50_ms}ms"
    echo "  Latency p95:  ${lat_p95_ms}ms"
    echo "  Latency p99:  ${lat_p99_ms}ms"
    echo ""

    # Return values via global variables
    _RESULT_SENT=$sent
    _RESULT_RECEIVED=$received
    _RESULT_CONFIRMED=$confirmed
    _RESULT_DUPLICATES=$duplicates
    _RESULT_GAPS=$gaps
    _RESULT_PUB_CRASHES=$pub_crashes
    _RESULT_SUB_CRASHES=$sub_crashes_count
    _RESULT_LOSS=$loss_pct
    _RESULT_LAT_P50=$lat_p50_ms
    _RESULT_LAT_P95=$lat_p95_ms
    _RESULT_LAT_P99=$lat_p99_ms
}

# Run comparison for a specific QoS level
run_qos_comparison() {
    local qos="$1"
    local test_id="$2"
    
    log_section "Testing QoS $qos (Both Pub & Sub Crash)"
    echo ""

    # Regular publisher with subscriber crashes
    run_single_test "regular_qos${qos}" "$qos" "pub" "$test_id" "true"
    local reg_sent=$_RESULT_SENT
    local reg_received=$_RESULT_RECEIVED
    local reg_duplicates=$_RESULT_DUPLICATES
    local reg_gaps=$_RESULT_GAPS
    local reg_pub_crashes=$_RESULT_PUB_CRASHES
    local reg_sub_crashes=$_RESULT_SUB_CRASHES
    local reg_loss=$_RESULT_LOSS
    local reg_lat_p50=$_RESULT_LAT_P50
    local reg_lat_p95=$_RESULT_LAT_P95
    local reg_lat_p99=$_RESULT_LAT_P99

    # Reliable publisher with subscriber crashes
    run_single_test "reliable_qos${qos}" "$qos" "rel-pub" "$test_id" "true"
    local rel_sent=$_RESULT_SENT
    local rel_received=$_RESULT_RECEIVED
    local rel_confirmed=$_RESULT_CONFIRMED
    local rel_duplicates=$_RESULT_DUPLICATES
    local rel_gaps=$_RESULT_GAPS
    local rel_pub_crashes=$_RESULT_PUB_CRASHES
    local rel_sub_crashes=$_RESULT_SUB_CRASHES
    local rel_loss=$_RESULT_LOSS
    local rel_lat_p50=$_RESULT_LAT_P50
    local rel_lat_p95=$_RESULT_LAT_P95
    local rel_lat_p99=$_RESULT_LAT_P99

    # Summary table for this QoS
    echo "========================================"
    echo "       QoS $qos Comparison Summary"
    echo "========================================"
    echo ""
    printf "%-20s %-12s %-12s\n" "Metric" "Regular" "Reliable"
    echo "--------------------------------------------"
    printf "%-20s %-12s %-12s\n" "Sent" "$reg_sent" "$rel_sent"
    printf "%-20s %-12s %-12s\n" "Received" "$reg_received" "$rel_received"
    printf "%-20s %-12s %-12s\n" "Duplicates" "$reg_duplicates" "$rel_duplicates"
    printf "%-20s %-12s %-12s\n" "Gaps" "$reg_gaps" "$rel_gaps"
    printf "%-20s %-12s %-12s\n" "Pub Crashes" "$reg_pub_crashes" "$rel_pub_crashes"
    printf "%-20s %-12s %-12s\n" "Sub Crashes" "$reg_sub_crashes" "$rel_sub_crashes"
    printf "%-20s %-12s %-12s\n" "Loss %" "${reg_loss}%" "${rel_loss}%"
    printf "%-20s %-12s %-12s\n" "Latency p50" "${reg_lat_p50}ms" "${rel_lat_p50}ms"
    printf "%-20s %-12s %-12s\n" "Latency p95" "${reg_lat_p95}ms" "${rel_lat_p95}ms"
    printf "%-20s %-12s %-12s\n" "Latency p99" "${reg_lat_p99}ms" "${rel_lat_p99}ms"
    echo "--------------------------------------------"
    echo ""

    # Verdict for this QoS
    if [[ "$rel_gaps" == "0" ]]; then
        log_info "✓ QoS $qos: Reliable publisher achieved ZERO gaps!"
    else
        log_warn "✗ QoS $qos: Reliable publisher had ${rel_gaps} gaps"
    fi

    if [[ "$rel_duplicates" -gt 0 ]]; then
        if [[ "$qos" == "1" ]]; then
            log_info "ℹ QoS $qos: ${rel_duplicates} duplicates (expected with at-least-once)"
        else
            log_warn "⚠ QoS $qos: ${rel_duplicates} duplicates (unexpected with exactly-once)"
        fi
    else
        log_info "✓ QoS $qos: ZERO duplicates"
    fi
    echo ""
}

main() {
    log_info "Reliable Publisher E2E Test (QoS 1 & 2)"
    log_info "========================================"
    echo ""
    log_info "Crashes happen on BOTH publisher AND subscriber."
    log_info "Using persistent sessions (clean_session=false)."
    echo ""

    check_prerequisites

    local test_id=$(uuidgen)
    log_info "Test ID: $test_id"
    log_info "Parameters:"
    echo "  Rate:        ${RATE}/s"
    echo "  Duration:    ${DURATION}s"
    echo "  Pub MTTF:    ${PUB_MTTF}s"
    echo "  Pub MTTR:    ${PUB_MTTR}s"
    echo "  Sub MTTF:    ${SUB_MTTF}s"
    echo "  Sub MTTR:    ${SUB_MTTR}s"
    echo "  Max crashes: ${CRASH_COUNT} per side"
    echo ""

    # Test QoS 1 (at-least-once)
    run_qos_comparison 1 "$test_id"

    # Test QoS 2 (exactly-once)
    run_qos_comparison 2 "$test_id"

    # Final summary
    echo ""
    echo "========================================"
    echo "           Overall Summary"
    echo "========================================"
    echo ""
    echo "With crashes on BOTH publisher and subscriber:"
    echo ""
    echo "QoS 1 (at-least-once):"
    echo "  • Pub crash: Reliable publisher re-sends unconfirmed messages"
    echo "  • Sub crash: Broker queues messages for persistent session"
    echo "  • May have duplicates (at-least-once semantics)"
    echo "  • SequenceTracker on subscriber filters duplicates"
    echo ""
    echo "QoS 2 (exactly-once):"
    echo "  • 4-step handshake: PUBLISH → PUBREC → PUBREL → PUBCOMP"
    echo "  • Broker tracks message IDs to prevent duplicates"
    echo "  • NOTE: QoS 2 with crash recovery may lose messages due to"
    echo "    rumqttc library not preserving transaction state across reconnects"
    echo "  • Recommendation: Use QoS 1 for crash-tolerant scenarios"
    echo ""
    echo "Artifacts saved to: $ARTIFACTS_DIR"
}

main "$@"
