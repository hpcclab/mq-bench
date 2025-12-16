#!/usr/bin/env bash
#
# Multi-topic MQTT failure test using mt-pub/mt-sub.
#
# Focus:
# - Exercise many topics/keys (closer to real workload)
# - Simulate failures using built-in crash injection on mt-sub
# - Optionally crash mt-pub process externally (process-level crash)
#
# Prereqs:
#   - Mosquitto broker on localhost:1883 (docker compose up -d mosquitto)
#   - cargo build --release

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="${PROJECT_DIR}/target/release/mq-bench"
ARTIFACTS_DIR="${PROJECT_DIR}/artifacts/mt_mqtt_failure_$(date +%Y%m%d_%H%M%S)"

BROKER_HOST="${BROKER_HOST:-127.0.0.1}"
BROKER_PORT="${BROKER_PORT:-1883}"

# Workload shape (keys = tenants*regions*services*shards)
TOPIC_PREFIX="${TOPIC_PREFIX:-bench/mtopic}"
TENANTS="${TENANTS:-1}"
REGIONS="${REGIONS:-2}"
SERVICES="${SERVICES:-5}"
SHARDS="${SHARDS:-10}"

# How many per-key pubs/subs to activate (-1 means all keys)
PUBLISHERS="${PUBLISHERS:--1}"
SUBSCRIBERS="${SUBSCRIBERS:--1}"

# Publish parameters
PAYLOAD="${PAYLOAD:-64}"
RATE="${RATE:-10}"          # per publisher msg/s
DURATION="${DURATION:-30}"

# Which QoS levels to run (space-separated), e.g. "1 2" or "1".
QOS_LEVELS="${QOS_LEVELS:-1 2}"

# Failure injection (subscriber built-in)
# IMPORTANT: force_disconnect() simulates HARD CRASH (no graceful disconnect).
# This can cause QoS message loss (especially QoS 2) because:
# - rumqttc loses all inflight QoS state on crash
# - On reconnect, broker expects session continuity but client has no memory
# - Result: "unsolicited ack" errors and message loss
#
# To reduce loss: increase SUB_MTTR >> broker session_expiry (e.g., 30s)
# OR: Use QOS_LEVELS=1 and accept some loss as expected behavior
SUB_MTTF="${SUB_MTTF:-5}"          # Longer MTTF = fewer crashes
SUB_MTTR="${SUB_MTTR:-3}"          # Longer MTTR = more realistic recovery
SUB_CRASH_COUNT="${SUB_CRASH_COUNT:-5}"  # Fewer crashes for cleaner test
SUB_CRASH_SEED="${SUB_CRASH_SEED:-54321}"

# Share transport flags (0 = per-key transports, 1 = shared transport)
# NOTE: Crash injection only works with per-topic transports (share_transport=0)
SUB_SHARE_TRANSPORT="${SUB_SHARE_TRANSPORT:-0}"
PUB_SHARE_TRANSPORT="${PUB_SHARE_TRANSPORT:-0}"

# Extra time (seconds) for subscriber to drain broker queue after publisher finishes.
# Subscriber runs for DURATION + SUB_EXTRA_DRAIN_SECS total.
SUB_EXTRA_DRAIN_SECS="${SUB_EXTRA_DRAIN_SECS:-15}"

# Optional: process-level publisher crash loop (0 disables)
PUB_CRASH_LOOP="${PUB_CRASH_LOOP:-0}"
PUB_MTTF="${PUB_MTTF:-1.5}"
PUB_MTTR="${PUB_MTTR:-1}"
PUB_CRASH_COUNT="${PUB_CRASH_COUNT:-10}"
PUB_CRASH_SEED="${PUB_CRASH_SEED:-12345}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_section() { echo -e "${CYAN}[====]${NC} $*"; }

cleanup() {
  jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT

check_prereq() {
  mkdir -p "$ARTIFACTS_DIR"
  if [[ ! -f "$BINARY" ]]; then
    log_error "Binary not found: $BINARY"
    log_info "Run: cargo build --release"
    exit 1
  fi
  if ! nc -z "$BROKER_HOST" "$BROKER_PORT" 2>/dev/null; then
    log_error "MQTT broker not running on $BROKER_HOST:$BROKER_PORT"
    log_info "Start with: docker compose up -d mosquitto"
    exit 1
  fi
  
  # Validate shared transport settings for crash injection
  if [[ "$SUB_MTTF" != "0" ]] && [[ "$SUB_SHARE_TRANSPORT" == "1" ]]; then
    log_warn "Subscriber crash injection requires share_transport=false, forcing SUB_SHARE_TRANSPORT=0"
    SUB_SHARE_TRANSPORT=0
  fi
  if [[ "$PUB_CRASH_LOOP" == "0" ]] && [[ "$PUB_SHARE_TRANSPORT" == "1" ]]; then
    # Publisher doesn't use built-in crash injection yet, only external PUB_CRASH_LOOP
    # But for consistency with subscriber, enforce per-topic for crash testing
    log_warn "Publisher crash testing works best with share_transport=false, forcing PUB_SHARE_TRANSPORT=0"
    PUB_SHARE_TRANSPORT=0
  fi
}

csv_last_field() {
  local csv_file="$1"
  local field_idx="$2"
  if [[ ! -f "$csv_file" ]]; then
    echo "0"
    return 0
  fi
  tail -n 1 "$csv_file" | cut -d',' -f"$field_idx" 2>/dev/null || echo "0"
}

parse_last_sent_from_pub_log() {
  local pub_log="$1"
  if [[ ! -f "$pub_log" ]]; then
    echo "0"
    return 0
  fi

  # mt-pub logs contain a final line like: "done sent=24066".
  local last
  last=$(grep -E ' done sent=' "$pub_log" | tail -n 1 || true)
  if [[ -z "$last" ]]; then
    echo "0"
    return 0
  fi
  echo "$last" | sed -n 's/.*sent=\([0-9][0-9]*\).*/\1/p' | head -n 1
}

parse_sum_sent_from_pub_log() {
  local pub_log="$1"
  if [[ ! -f "$pub_log" ]]; then
    echo "0"
    return 0
  fi
  # If PUB_CRASH_LOOP=1, multiple mt-pub runs may happen. Sum all "done sent=" values.
  grep -E ' done sent=' "$pub_log" \
    | sed -n 's/.*sent=\([0-9][0-9]*\).*/\1/p' \
    | awk '{s+=$1} END{print s+0}'
}

ns_to_ms() {
  local ns="$1"
  awk -v ns="$ns" 'BEGIN{ if (ns+0 <= 0) { printf("0.00"); } else { printf("%.2f", ns/1000000.0); } }'
}

# Simple deterministic xorshift64 PRNG in bash for crash loop timing
xorshift64() {
  local x="$1"
  x=$(( x ^ (x << 13) ))
  x=$(( x ^ (x >> 7) ))
  x=$(( x ^ (x << 17) ))
  echo "$x"
}

# Sample a truncated exponential in seconds (cap at 3x mean)
# Uses integer math milliseconds for portability.
exp_sample_ms() {
  local mean_s="$1"
  local state="$2"
  state=$(xorshift64 "$state")
  # U in (0,1): use lower 53 bits like a float fraction
  local u=$(( (state & ((1<<53)-1)) ))
  if [[ "$u" -le 0 ]]; then u=1; fi
  # Approx ln via awk for simplicity
  local ms
  ms=$(awk -v mean="$mean_s" -v u="$u" 'BEGIN{
    U = u / (2^53);
    if (U < 1e-10) U = 1e-10;
    s = -mean * log(U);
    if (s > mean*3.0) s = mean*3.0;
    printf("%d", s*1000.0);
  }')
  echo "$ms $state"
}

run_pub_crash_loop() {
  local pub_cmd=("$@")
  local state="$PUB_CRASH_SEED"
  local crashes=0

  while true; do
    "${pub_cmd[@]}" &
    local pid=$!

    # time to failure
    local out
    out=$(exp_sample_ms "$PUB_MTTF" "$state")
    local ttf_ms=${out%% *}
    state=${out##* }

    sleep "$(awk -v ms="$ttf_ms" 'BEGIN{printf("%.3f", ms/1000.0)}')"

    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid" 2>/dev/null || true
    else
      kill -9 "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi

    crashes=$((crashes + 1))
    if [[ "$PUB_CRASH_COUNT" -gt 0 && "$crashes" -ge "$PUB_CRASH_COUNT" ]]; then
      break
    fi

    # repair
    out=$(exp_sample_ms "$PUB_MTTR" "$state")
    local rt_ms=${out%% *}
    state=${out##* }
    sleep "$(awk -v ms="$rt_ms" 'BEGIN{printf("%.3f", ms/1000.0)}')"
  done
}

run_single_test() {
  local qos="$1"
  local test_id="$2"

  local run_id
  run_id=$(uuidgen)

  local sub_csv="$ARTIFACTS_DIR/mt_sub_qos${qos}.csv"
  local sub_log="$ARTIFACTS_DIR/mt_sub_qos${qos}.log"
  local pub_csv="$ARTIFACTS_DIR/mt_pub_qos${qos}.csv"
  local pub_log="$ARTIFACTS_DIR/mt_pub_qos${qos}.log"

  local sub_share_flag=()
  if [[ "$SUB_SHARE_TRANSPORT" == "1" ]]; then
    sub_share_flag=(--share-transport)
  fi

  local pub_share_flag=()
  if [[ "$PUB_SHARE_TRANSPORT" == "1" ]]; then
    pub_share_flag=(--share-transport)
  fi

  # Subscriber runs longer than publisher to drain broker queue
  local sub_duration=$((DURATION + SUB_EXTRA_DRAIN_SECS))

  # Start mt-sub with built-in crash injection
  "$BINARY" mt-sub \
    --engine mqtt \
    --connect "host=$BROKER_HOST" \
    --connect "port=$BROKER_PORT" \
    --connect "client_id=mt-sub-$run_id" \
    --connect "qos=$qos" \
    --connect "clean_session=false" \
    --topic-prefix "$TOPIC_PREFIX/$run_id" \
    --tenants "$TENANTS" --regions "$REGIONS" --services "$SERVICES" --shards "$SHARDS" \
    --subscribers "$SUBSCRIBERS" \
    --mapping mdim \
    --duration "$sub_duration" \
    "${sub_share_flag[@]}" \
    --csv "$sub_csv" \
    --enable-retry \
    --mttf "$SUB_MTTF" --mttr "$SUB_MTTR" --crash-count "$SUB_CRASH_COUNT" --crash-seed "$SUB_CRASH_SEED" \
    >"$sub_log" 2>&1 &
  local sub_pid=$!

  # Give subs time to connect
  sleep 2

  local pub_cmd=(
    "$BINARY" mt-pub
    --engine mqtt
    --connect "host=$BROKER_HOST"
    --connect "port=$BROKER_PORT"
    --connect "client_id=mt-pub-$run_id"
    --connect "qos=$qos"
    --connect "clean_session=false"
    --topic-prefix "$TOPIC_PREFIX/$run_id"
    --tenants "$TENANTS" --regions "$REGIONS" --services "$SERVICES" --shards "$SHARDS"
    --publishers "$PUBLISHERS"
    --mapping mdim
    --payload "$PAYLOAD"
    --rate "$RATE"
    --duration "$DURATION"
    "${pub_share_flag[@]}"
    --csv "$pub_csv"
    --enable-retry
  )

  if [[ "$PUB_CRASH_LOOP" == "1" ]]; then
    log_warn "Publisher process crash loop enabled"
    run_pub_crash_loop "${pub_cmd[@]}" >"$pub_log" 2>&1
  else
    "${pub_cmd[@]}" >"$pub_log" 2>&1
  fi

  # Wait for subscriber to finish
  wait "$sub_pid" 2>/dev/null || true

  # Gather metrics (prefer CSV)
  local sent=0
  if [[ "$PUB_CRASH_LOOP" == "1" ]]; then
    sent=$(parse_sum_sent_from_pub_log "$pub_log")
  else
    # Prefer the final log line ("done sent=") to avoid using an intermediate CSV snapshot.
    sent=$(parse_last_sent_from_pub_log "$pub_log")
    if [[ "$sent" == "0" ]]; then
      sent=$(csv_last_field "$pub_csv" 2)
    fi
  fi

  local received
  received=$(csv_last_field "$sub_csv" 3)
  local errors
  errors=$(csv_last_field "$sub_csv" 4)
  local dup
  dup=$(csv_last_field "$sub_csv" 20)
  local gaps
  gaps=$(csv_last_field "$sub_csv" 21)
  local sub_crashes
  sub_crashes=$(csv_last_field "$sub_csv" 17)
  local sub_reconnects
  sub_reconnects=$(csv_last_field "$sub_csv" 18)
  local sub_reconnect_failures
  sub_reconnect_failures=$(csv_last_field "$sub_csv" 19)

  local lat_p50_ns lat_p95_ns lat_p99_ns
  lat_p50_ns=$(csv_last_field "$sub_csv" 7)
  lat_p95_ns=$(csv_last_field "$sub_csv" 8)
  lat_p99_ns=$(csv_last_field "$sub_csv" 9)
  local lat_p50_ms lat_p95_ms lat_p99_ms
  lat_p50_ms=$(ns_to_ms "$lat_p50_ns")
  lat_p95_ms=$(ns_to_ms "$lat_p95_ns")
  lat_p99_ms=$(ns_to_ms "$lat_p99_ns")

  local loss_pct="N/A"
  if [[ "$sent" -gt 0 ]]; then
    loss_pct=$(echo "scale=1; ($sent - $received) * 100 / $sent" | bc 2>/dev/null || echo "0")
  fi

  _MT_SENT=$sent
  _MT_RECEIVED=$received
  _MT_ERRORS=$errors
  _MT_DUPLICATES=$dup
  _MT_GAPS=$gaps
  _MT_SUB_CRASHES=$sub_crashes
  _MT_SUB_RECONNECTS=$sub_reconnects
  _MT_SUB_RECONNECT_FAILURES=$sub_reconnect_failures
  _MT_LOSS=$loss_pct
  _MT_LAT_P50=$lat_p50_ms
  _MT_LAT_P95=$lat_p95_ms
  _MT_LAT_P99=$lat_p99_ms
  _MT_SUB_LOG=$sub_log
  _MT_PUB_LOG=$pub_log
  _MT_SUB_CSV=$sub_csv
  _MT_PUB_CSV=$pub_csv
}

run_qos_comparison() {
  local qos="$1"
  local test_id="$2"

  log_section "Testing QoS $qos (multi-topic)"
  run_single_test "$qos" "$test_id"

  echo "  Sent:              $_MT_SENT"
  echo "  Received:          $_MT_RECEIVED"
  echo "  Errors:            $_MT_ERRORS"
  echo "  Duplicates:        $_MT_DUPLICATES"
  echo "  Gaps:              $_MT_GAPS"
  echo "  Sub Crashes:       $_MT_SUB_CRASHES"
  echo "  Sub Reconnects:    $_MT_SUB_RECONNECTS"
  echo "  Reconnect Fail:    $_MT_SUB_RECONNECT_FAILURES"
  echo "  Loss (sent):       ${_MT_LOSS}%"
  echo "  Latency p50:       ${_MT_LAT_P50}ms"
  echo "  Latency p95:       ${_MT_LAT_P95}ms"
  echo "  Latency p99:       ${_MT_LAT_P99}ms"
  echo "  Logs:              $_MT_SUB_LOG $_MT_PUB_LOG"
  echo "  CSV:               $_MT_SUB_CSV $_MT_PUB_CSV"
  echo ""
}

main() {
  check_prereq

  log_section "MQTT multi-topic failure test"
  log_info "Artifacts: $ARTIFACTS_DIR"
  log_info "Keys: T=$TENANTS R=$REGIONS S=$SERVICES K=$SHARDS"
  log_info "QoS levels: $QOS_LEVELS"
  log_info "payload=$PAYLOAD duration=${DURATION}s rate(per-pub)=${RATE}/s"
  echo ""

  local test_id
  test_id=$(uuidgen)
  log_info "Test ID: $test_id"
  echo ""

  # Run each QoS level
  local qos
  for qos in $QOS_LEVELS; do
    run_qos_comparison "$qos" "$test_id"

    # Save per-QoS summary values
    if [[ "$qos" == "1" ]]; then
      qos1_sent=$_MT_SENT; qos1_recv=$_MT_RECEIVED; qos1_err=$_MT_ERRORS
      qos1_dup=$_MT_DUPLICATES; qos1_gaps=$_MT_GAPS
      qos1_crash=$_MT_SUB_CRASHES; qos1_reconn=$_MT_SUB_RECONNECTS
      qos1_loss=$_MT_LOSS; qos1_p50=$_MT_LAT_P50; qos1_p95=$_MT_LAT_P95; qos1_p99=$_MT_LAT_P99
    elif [[ "$qos" == "2" ]]; then
      qos2_sent=$_MT_SENT; qos2_recv=$_MT_RECEIVED; qos2_err=$_MT_ERRORS
      qos2_dup=$_MT_DUPLICATES; qos2_gaps=$_MT_GAPS
      qos2_crash=$_MT_SUB_CRASHES; qos2_reconn=$_MT_SUB_RECONNECTS
      qos2_loss=$_MT_LOSS; qos2_p50=$_MT_LAT_P50; qos2_p95=$_MT_LAT_P95; qos2_p99=$_MT_LAT_P99
    fi
  done

  # Comparison table if both QoS 1 and 2 were run
  if echo " $QOS_LEVELS " | grep -q " 1 " && echo " $QOS_LEVELS " | grep -q " 2 "; then
    echo "========================================"
    echo "     Multi-topic QoS Comparison Summary"
    echo "========================================"
    echo ""
    printf "%-20s %-12s %-12s\n" "Metric" "QoS1" "QoS2"
    echo "--------------------------------------------"
    printf "%-20s %-12s %-12s\n" "Sent" "${qos1_sent:-0}" "${qos2_sent:-0}"
    printf "%-20s %-12s %-12s\n" "Received" "${qos1_recv:-0}" "${qos2_recv:-0}"
    printf "%-20s %-12s %-12s\n" "Errors" "${qos1_err:-0}" "${qos2_err:-0}"
    printf "%-20s %-12s %-12s\n" "Duplicates" "${qos1_dup:-0}" "${qos2_dup:-0}"
    printf "%-20s %-12s %-12s\n" "Gaps" "${qos1_gaps:-0}" "${qos2_gaps:-0}"
    printf "%-20s %-12s %-12s\n" "Sub Crashes" "${qos1_crash:-0}" "${qos2_crash:-0}"
    printf "%-20s %-12s %-12s\n" "Reconnects" "${qos1_reconn:-0}" "${qos2_reconn:-0}"
    printf "%-20s %-12s %-12s\n" "Loss %" "${qos1_loss:-N/A}%" "${qos2_loss:-N/A}%"
    printf "%-20s %-12s %-12s\n" "Latency p50" "${qos1_p50:-0}ms" "${qos2_p50:-0}ms"
    printf "%-20s %-12s %-12s\n" "Latency p95" "${qos1_p95:-0}ms" "${qos2_p95:-0}ms"
    printf "%-20s %-12s %-12s\n" "Latency p99" "${qos1_p99:-0}ms" "${qos2_p99:-0}ms"
    echo "--------------------------------------------"
    echo ""
  fi

  echo "Artifacts saved to: $ARTIFACTS_DIR"
}

main "$@"
