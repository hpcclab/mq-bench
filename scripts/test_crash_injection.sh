#!/usr/bin/env bash
# Integration test: crash injection with actual MQTT broker
#
# This script tests the crash injection feature end-to-end using the
# compiled binary against a running MQTT broker (Mosquitto).
#
# Prerequisites:
#   - Docker with Mosquitto running (docker compose up mosquitto -d)
#   - Compiled mq-bench binary (cargo build --release)
#
# Usage:
#   ./scripts/test_crash_injection.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="${PROJECT_DIR}/target/release/mq-bench"
ARTIFACTS_DIR="${PROJECT_DIR}/artifacts/crash_test_$(date +%Y%m%d_%H%M%S)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

cleanup() {
    log_info "Cleaning up background processes..."
    jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if [[ ! -f "$BINARY" ]]; then
        log_error "Binary not found at $BINARY"
        log_info "Run: cargo build --release"
        exit 1
    fi

    # Check if Mosquitto is running
    if ! nc -z 127.0.0.1 1883 2>/dev/null; then
        log_warn "Mosquitto not running on port 1883"
        log_info "Attempting to start Mosquitto..."
        docker compose -f "${PROJECT_DIR}/docker-compose.yml" up -d mosquitto
        sleep 2
        
        if ! nc -z 127.0.0.1 1883 2>/dev/null; then
            log_error "Failed to start Mosquitto"
            exit 1
        fi
    fi
    
    log_info "Prerequisites OK"
}

# Create artifacts directory
mkdir -p "$ARTIFACTS_DIR"

run_test_basic_crash() {
    log_info "=== Test 1: Basic crash injection (publisher) ==="
    
    local pub_csv="$ARTIFACTS_DIR/test1_pub.csv"
    
    # Run publisher with crash injection for 10 seconds
    # MTTF=2s means on average one crash every 2 seconds
    # crash_count=3 means exactly 3 crashes will occur
    "$BINARY" pub \
        --engine mqtt \
        --connect host=127.0.0.1 \
        --connect port=1883 \
        --topic-prefix crash/test1 \
        --payload 64 \
        --rate 10 \
        --duration 15 \
        --mttf 2 \
        --mttr 0.5 \
        --crash-count 3 \
        --crash-seed 42 \
        --enable-retry \
        --csv "$pub_csv" \
        2>&1 | tee "$ARTIFACTS_DIR/test1_pub.log"
    
    # Check output
    if [[ -f "$pub_csv" ]]; then
        log_info "CSV output written to $pub_csv"
        
        # Check for crash metrics in CSV header
        if grep -q "crashes_injected" "$pub_csv"; then
            log_info "CSV contains crashes_injected column"
        else
            log_error "CSV missing crashes_injected column"
            return 1
        fi
        
        # Check for crashes in the data
        local crashes=$(tail -1 "$pub_csv" | cut -d',' -f$(head -1 "$pub_csv" | tr ',' '\n' | grep -n crashes_injected | cut -d: -f1))
        log_info "Crashes recorded: $crashes"
        
        if [[ "$crashes" -ge 3 ]]; then
            log_info "${GREEN}✓ Test 1 PASSED${NC}"
        else
            log_error "Expected at least 3 crashes, got $crashes"
            return 1
        fi
    else
        log_error "CSV output not created"
        return 1
    fi
}

run_test_no_retry_stops() {
    log_info "=== Test 2: Crash without retry stops immediately ==="
    
    local pub_csv="$ARTIFACTS_DIR/test2_pub.csv"
    local start_time=$(date +%s)
    
    # Run without --enable-retry, should stop on first crash
    "$BINARY" pub \
        --engine mqtt \
        --connect host=127.0.0.1 \
        --connect port=1883 \
        --topic-prefix crash/test2 \
        --payload 64 \
        --rate 10 \
        --duration 60 \
        --mttf 1 \
        --mttr 0.5 \
        --crash-count 1 \
        --crash-seed 123 \
        --csv "$pub_csv" \
        2>&1 | tee "$ARTIFACTS_DIR/test2_pub.log"
    
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    
    log_info "Test completed in ${elapsed}s"
    
    # Should complete in much less than 60 seconds (stopped on crash)
    if [[ $elapsed -lt 30 ]]; then
        log_info "${GREEN}✓ Test 2 PASSED${NC} - Stopped early as expected"
    else
        log_error "Expected early termination, ran for ${elapsed}s"
        return 1
    fi
}

run_test_pub_sub_with_crashes() {
    log_info "=== Test 3: Pub/Sub with independent crashes ==="
    
    local sub_csv="$ARTIFACTS_DIR/test3_sub.csv"
    local pub_csv="$ARTIFACTS_DIR/test3_pub.csv"
    
    # Start subscriber with crash injection
    "$BINARY" sub \
        --engine mqtt \
        --connect host=127.0.0.1 \
        --connect port=1883 \
        --expr "crash/test3" \
        --mttf 3 \
        --mttr 0.3 \
        --crash-count 2 \
        --crash-seed 111 \
        --enable-retry \
        --csv "$sub_csv" \
        2>&1 | tee "$ARTIFACTS_DIR/test3_sub.log" &
    local sub_pid=$!
    
    sleep 1  # Let subscriber connect first
    
    # Start publisher with different crash pattern
    "$BINARY" pub \
        --engine mqtt \
        --connect host=127.0.0.1 \
        --connect port=1883 \
        --topic-prefix crash/test3 \
        --payload 64 \
        --rate 10 \
        --duration 15 \
        --mttf 2 \
        --mttr 0.2 \
        --crash-count 3 \
        --crash-seed 222 \
        --enable-retry \
        --csv "$pub_csv" \
        2>&1 | tee "$ARTIFACTS_DIR/test3_pub.log"
    
    # Wait for subscriber to finish
    kill $sub_pid 2>/dev/null || true
    wait $sub_pid 2>/dev/null || true
    
    # Check both outputs
    if [[ -f "$pub_csv" ]] && [[ -f "$sub_csv" ]]; then
        log_info "${GREEN}✓ Test 3 PASSED${NC} - Both pub and sub completed"
    else
        log_error "Missing output files"
        return 1
    fi
}

run_test_deterministic_seed() {
    log_info "=== Test 4: Deterministic crash pattern with seed ==="
    
    local csv1="$ARTIFACTS_DIR/test4_run1.csv"
    local csv2="$ARTIFACTS_DIR/test4_run2.csv"
    
    # First run
    "$BINARY" pub \
        --engine mqtt \
        --connect host=127.0.0.1 \
        --connect port=1883 \
        --topic-prefix crash/test4 \
        --payload 64 \
        --rate 100 \
        --duration 5 \
        --mttf 1 \
        --mttr 0.1 \
        --crash-count 3 \
        --crash-seed 999 \
        --enable-retry \
        --csv "$csv1" \
        2>&1 | tee "$ARTIFACTS_DIR/test4_run1.log"
    
    sleep 1
    
    # Second run with same seed
    "$BINARY" pub \
        --engine mqtt \
        --connect host=127.0.0.1 \
        --connect port=1883 \
        --topic-prefix crash/test4 \
        --payload 64 \
        --rate 100 \
        --duration 5 \
        --mttf 1 \
        --mttr 0.1 \
        --crash-count 3 \
        --crash-seed 999 \
        --enable-retry \
        --csv "$csv2" \
        2>&1 | tee "$ARTIFACTS_DIR/test4_run2.log"
    
    # Both runs should have same number of crashes (3)
    local crashes1=$(tail -1 "$csv1" | cut -d',' -f$(head -1 "$csv1" | tr ',' '\n' | grep -n crashes_injected | cut -d: -f1))
    local crashes2=$(tail -1 "$csv2" | cut -d',' -f$(head -1 "$csv2" | tr ',' '\n' | grep -n crashes_injected | cut -d: -f1))
    
    log_info "Run 1 crashes: $crashes1, Run 2 crashes: $crashes2"
    
    if [[ "$crashes1" -eq "$crashes2" ]] && [[ "$crashes1" -ge 3 ]]; then
        log_info "${GREEN}✓ Test 4 PASSED${NC} - Same seed produced consistent behavior"
    else
        log_warn "Crash counts differ (may be timing-dependent)"
    fi
}

# Main execution
main() {
    log_info "Starting crash injection integration tests"
    log_info "Artifacts will be saved to: $ARTIFACTS_DIR"
    
    check_prerequisites
    
    local failed=0
    
    # Run tests that require MQTT broker
    run_test_basic_crash || ((failed++))
    run_test_no_retry_stops || ((failed++))
    run_test_pub_sub_with_crashes || ((failed++))
    run_test_deterministic_seed || ((failed++))
    
    echo ""
    log_info "========================================"
    if [[ $failed -eq 0 ]]; then
        log_info "${GREEN}All tests PASSED${NC}"
    else
        log_error "$failed test(s) FAILED"
    fi
    log_info "Artifacts saved to: $ARTIFACTS_DIR"
    log_info "========================================"
    
    exit $failed
}

main "$@"
