#!/usr/bin/env bash
#
# Multi-topic MQTT network failure test using Toxiproxy.
#
# Unlike process crash simulation, this script uses Toxiproxy to inject
# REAL network failures:
#   - reset_peer: TCP connection reset (simulates network drop)
#   - timeout: Network partition (packets blackholed)
#   - latency: Network delay
#   - limit_data: Bandwidth limit then close
#
# This provides more realistic MQTT failure scenarios than process crashes.
#
# KEY DESIGN: Separate proxies for publisher and subscriber
#   - Publisher uses port 21xxx (clean proxy, no faults injected)
#   - Subscriber uses port 22xxx (faulted proxy, failures injected here)
#
# This allows the publisher to keep sending to the broker while only the
# subscriber experiences network failures. The broker buffers messages
# (QoS 1/2), and the subscriber receives them after reconnecting.
#
# Prereqs:
#   - MQTT broker running (docker compose up -d mosquitto)
#   - Toxiproxy running with proxy configured (see config/toxiproxy.json)
#   - cargo build --release
#
# Usage:
#   # Start toxiproxy first:
#   docker run -d --name toxiproxy --net=host \
#     -v $(pwd)/config/toxiproxy.json:/config.json \
#     ghcr.io/shopify/toxiproxy -config /config.json
#
#   # Then run test:
#   scripts/test_mqtt_network_failure.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="${PROJECT_DIR}/target/release/mq-bench"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-${PROJECT_DIR}/artifacts/mt_mqtt_netfail_$(date +%Y%m%d_%H%M%S)}"

# Toxiproxy API endpoint
TOXIPROXY_API="${TOXIPROXY_API:-http://127.0.0.1:8474}"

# The proxy names in toxiproxy (must match config/toxiproxy.json)
# Publisher proxy (clean - no faults)
TOXIPROXY_PUB="${TOXIPROXY_PUB:-mqtt_mosquitto_pub}"
# Subscriber proxy (faulted - failures injected here)
TOXIPROXY_SUB="${TOXIPROXY_SUB:-mqtt_mosquitto_sub}"

# Broker settings (connect via toxiproxy ports, not direct)
BROKER_HOST="${BROKER_HOST:-127.0.0.1}"
# Publisher port (clean proxy, 21xxx)
PUB_BROKER_PORT="${PUB_BROKER_PORT:-21883}"
# Subscriber port (faulted proxy, 22xxx)
SUB_BROKER_PORT="${SUB_BROKER_PORT:-22883}"

# Workload shape
TOPIC_PREFIX="${TOPIC_PREFIX:-bench/netfail}"
TENANTS="${TENANTS:-1}"
REGIONS="${REGIONS:-2}"
SERVICES="${SERVICES:-5}"
SHARDS="${SHARDS:-10}"

PUBLISHERS="${PUBLISHERS:--1}"
SUBSCRIBERS="${SUBSCRIBERS:--1}"

PAYLOAD="${PAYLOAD:-1024}"
RATE="${RATE:-1}"
DURATION="${DURATION:-180}"

QOS_LEVELS="${QOS_LEVELS:-0 1 2}"

# Network failure injection parameters
# Failure types: reset_peer, timeout, latency, limit_data
FAILURE_TYPE="${FAILURE_TYPE:-reset_peer}"

# Mean Time To Failure / Repair (seconds)
NET_MTTF="${NET_MTTF:-30}"
NET_MTTR="${NET_MTTR:-5}"
NET_FAILURE_COUNT="${NET_FAILURE_COUNT:-100}"
NET_FAILURE_SEED="${NET_FAILURE_SEED:-54321}"

# Latency injection (only used when FAILURE_TYPE=latency)
LATENCY_MS="${LATENCY_MS:-500}"
LATENCY_JITTER_MS="${LATENCY_JITTER_MS:-100}"

# Extra subscriber drain time
SUB_EXTRA_DRAIN_SECS="${SUB_EXTRA_DRAIN_SECS:-15}"

# Share transport (1 = shared, 0 = per-topic)
SUB_SHARE_TRANSPORT="${SUB_SHARE_TRANSPORT:-0}"
PUB_SHARE_TRANSPORT="${PUB_SHARE_TRANSPORT:-0}"

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
  log_info "Cleaning up..."
  # Remove any active toxics
  remove_all_toxics >/dev/null 2>&1 || true
  # Kill background jobs
  jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT

#=============================================================================
# Toxiproxy API functions
#=============================================================================

toxiproxy_api() {
  local method="$1"
  local path="$2"
  local data="${3:-}"
  
  if [[ -n "$data" ]]; then
    curl -s -X "$method" "${TOXIPROXY_API}${path}" \
      -H "Content-Type: application/json" \
      -d "$data"
  else
    curl -s -X "$method" "${TOXIPROXY_API}${path}"
  fi
}

check_toxiproxy() {
  if ! curl -s "${TOXIPROXY_API}/version" >/dev/null 2>&1; then
    log_error "Toxiproxy not reachable at ${TOXIPROXY_API}"
    log_info "Start with: docker run -d --name toxiproxy --net=host \\"
    log_info "  -v \$(pwd)/config/toxiproxy.json:/config.json \\"
    log_info "  ghcr.io/shopify/toxiproxy -config /config.json"
    return 1
  fi
  
  # Check if both proxies exist (pub = clean, sub = faulted)
  local proxies
  proxies=$(toxiproxy_api GET "/proxies")
  
  if ! echo "$proxies" | grep -q "\"$TOXIPROXY_PUB\""; then
    log_error "Publisher proxy '$TOXIPROXY_PUB' not found in Toxiproxy"
    log_info "Available proxies: $(echo "$proxies" | jq -r 'keys | join(", ")')"
    return 1
  fi
  
  if ! echo "$proxies" | grep -q "\"$TOXIPROXY_SUB\""; then
    log_error "Subscriber proxy '$TOXIPROXY_SUB' not found in Toxiproxy"
    log_info "Available proxies: $(echo "$proxies" | jq -r 'keys | join(", ")')"
    return 1
  fi
  
  log_info "Toxiproxy OK: pub=$TOXIPROXY_PUB (port $PUB_BROKER_PORT), sub=$TOXIPROXY_SUB (port $SUB_BROKER_PORT)"
  return 0
}

# Add a toxic to the subscriber proxy (only sub is faulted)
add_toxic() {
  local toxic_type="$1"
  local toxic_name="$2"
  local attributes="$3"
  local stream="${4:-downstream}"  # downstream or upstream
  
  local payload
  payload=$(cat <<EOF
{
  "name": "$toxic_name",
  "type": "$toxic_type",
  "stream": "$stream",
  "attributes": $attributes
}
EOF
)
  
  toxiproxy_api POST "/proxies/${TOXIPROXY_SUB}/toxics" "$payload"
}

# Remove a specific toxic from subscriber proxy
remove_toxic() {
  local toxic_name="$1"
  toxiproxy_api DELETE "/proxies/${TOXIPROXY_SUB}/toxics/${toxic_name}"
}

# Remove all toxics from the subscriber proxy
remove_all_toxics() {
  local toxics
  toxics=$(toxiproxy_api GET "/proxies/${TOXIPROXY_SUB}/toxics")
  
  if [[ "$toxics" == "[]" ]] || [[ -z "$toxics" ]]; then
    return 0
  fi
  
  echo "$toxics" | jq -r '.[].name' | while read -r name; do
    remove_toxic "$name" >/dev/null 2>&1 || true
  done
}

# Inject a network failure based on FAILURE_TYPE
inject_failure() {
  local failure_id="$1"
  
  case "$FAILURE_TYPE" in
    reset_peer)
      # Immediately reset the connection after timeout (simulates abrupt disconnect)
      add_toxic "reset_peer" "failure_${failure_id}" '{"timeout": 0}'
      ;;
    timeout)
      # Blackhole packets (network partition)
      add_toxic "timeout" "failure_${failure_id}" '{"timeout": 0}'
      ;;
    latency)
      # Add significant latency
      add_toxic "latency" "failure_${failure_id}" \
        "{\"latency\": ${LATENCY_MS}, \"jitter\": ${LATENCY_JITTER_MS}}"
      ;;
    limit_data)
      # Close connection after transferring some bytes
      add_toxic "limit_data" "failure_${failure_id}" '{"bytes": 1024}'
      ;;
    *)
      log_error "Unknown failure type: $FAILURE_TYPE"
      return 1
      ;;
  esac
}

# Remove a network failure
remove_failure() {
  local failure_id="$1"
  remove_toxic "failure_${failure_id}" >/dev/null 2>&1 || true
}

#=============================================================================
# PRNG for failure scheduling (same as original script)
#=============================================================================

xorshift64() {
  local x="$1"
  x=$(( x ^ (x << 13) ))
  x=$(( x ^ (x >> 7) ))
  x=$(( x ^ (x << 17) ))
  echo "$x"
}

exp_sample_ms() {
  local mean_s="$1"
  local state="$2"
  state=$(xorshift64 "$state")
  local u=$(( (state & ((1<<53)-1)) ))
  if [[ "$u" -le 0 ]]; then u=1; fi
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

#=============================================================================
# Network failure injection loop (runs in background)
#=============================================================================

run_failure_injection_loop() {
  local duration="$1"
  local start_time
  start_time=$(date +%s)
  local end_time=$((start_time + duration))
  
  local state="$NET_FAILURE_SEED"
  local failures=0
  local failure_log="${ARTIFACTS_DIR}/network_failures.log"
  
  echo "# Network Failure Injection Log" > "$failure_log"
  echo "# Type: $FAILURE_TYPE, MTTF: ${NET_MTTF}s, MTTR: ${NET_MTTR}s" >> "$failure_log"
  echo "# timestamp,event,failure_id,duration_ms" >> "$failure_log"
  
  while [[ $(date +%s) -lt $end_time ]]; do
    # Time to failure
    local out
    out=$(exp_sample_ms "$NET_MTTF" "$state")
    local ttf_ms=${out%% *}
    state=${out##* }
    
    sleep "$(awk -v ms="$ttf_ms" 'BEGIN{printf("%.3f", ms/1000.0)}')"
    
    # Check if we've exceeded duration
    if [[ $(date +%s) -ge $end_time ]]; then
      break
    fi
    
    # Inject failure
    failures=$((failures + 1))
    local ts
    ts=$(date +%s.%N)
    
    if inject_failure "$failures" >/dev/null 2>&1; then
      echo "${ts},INJECT,${failures},0" >> "$failure_log"
      log_warn "[INJECT] Network failure #$failures ($FAILURE_TYPE)"
    else
      log_error "[INJECT] Failed to inject failure #$failures"
    fi
    
    # Time to repair
    out=$(exp_sample_ms "$NET_MTTR" "$state")
    local ttr_ms=${out%% *}
    state=${out##* }
    
    sleep "$(awk -v ms="$ttr_ms" 'BEGIN{printf("%.3f", ms/1000.0)}')"
    
    # Remove failure (network recovery)
    ts=$(date +%s.%N)
    if remove_failure "$failures" >/dev/null 2>&1; then
      echo "${ts},RECOVER,${failures},${ttr_ms}" >> "$failure_log"
      log_info "[RECOVER] Network recovered from failure #$failures (was down ${ttr_ms}ms)"
    fi
    
    if [[ "$NET_FAILURE_COUNT" -gt 0 && "$failures" -ge "$NET_FAILURE_COUNT" ]]; then
      log_info "Reached max failure count: $failures"
      break
    fi
  done
  
  echo "# Total failures injected: $failures" >> "$failure_log"
  log_info "Failure injection complete: $failures failures"
}

#=============================================================================
# Utility functions
#=============================================================================

check_prereq() {
  mkdir -p "$ARTIFACTS_DIR"
  
  if [[ ! -f "$BINARY" ]]; then
    log_error "Binary not found: $BINARY"
    log_info "Run: cargo build --release"
    exit 1
  fi
  
  if ! check_toxiproxy; then
    exit 1
  fi
  
  # Also check direct broker connectivity (through both proxies)
  if ! nc -z "$BROKER_HOST" "$PUB_BROKER_PORT" 2>/dev/null; then
    log_error "Cannot connect to pub proxy on $BROKER_HOST:$PUB_BROKER_PORT"
    exit 1
  fi
  if ! nc -z "$BROKER_HOST" "$SUB_BROKER_PORT" 2>/dev/null; then
    log_error "Cannot connect to sub proxy on $BROKER_HOST:$SUB_BROKER_PORT"
    exit 1
  fi
  
  # Ensure no leftover toxics
  remove_all_toxics
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
  local last
  last=$(grep -E ' done sent=' "$pub_log" | tail -n 1 || true)
  if [[ -z "$last" ]]; then
    echo "0"
    return 0
  fi
  echo "$last" | sed -n 's/.*sent=\([0-9][0-9]*\).*/\1/p' | head -n 1
}

ns_to_ms() {
  local ns="$1"
  awk -v ns="$ns" 'BEGIN{ if (ns+0 <= 0) { printf("0.00"); } else { printf("%.2f", ns/1000000.0); } }'
}

#=============================================================================
# Test execution
#=============================================================================

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

  local sub_duration=$((DURATION + SUB_EXTRA_DRAIN_SECS))

  # Build credential flags if set
  local cred_flags=()
  if [[ -n "${MQTT_USERNAME:-}" ]]; then
    cred_flags+=(--connect "username=${MQTT_USERNAME}")
  fi
  if [[ -n "${MQTT_PASSWORD:-}" ]]; then
    cred_flags+=(--connect "password=${MQTT_PASSWORD}")
  fi

  # Start subscriber via FAULTED proxy (port 22xxx)
  # Network failures will be injected on this proxy only
  "$BINARY" mt-sub \
    --engine mqtt \
    --connect "host=$BROKER_HOST" \
    --connect "port=$SUB_BROKER_PORT" \
    --connect "client_id=mt-sub-$run_id" \
    --connect "qos=$qos" \
    --connect "clean_session=false" \
    "${cred_flags[@]}" \
    --topic-prefix "$TOPIC_PREFIX/$run_id" \
    --tenants "$TENANTS" --regions "$REGIONS" --services "$SERVICES" --shards "$SHARDS" \
    --subscribers "$SUBSCRIBERS" \
    --mapping mdim \
    --duration "$sub_duration" \
    "${sub_share_flag[@]}" \
    --csv "$sub_csv" \
    --enable-retry \
    >"$sub_log" 2>&1 &
  local sub_pid=$!

  sleep 2  # Let subscriber connect

  # Start publisher via CLEAN proxy (port 21xxx)
  # Publisher is not affected by network failures - keeps sending to broker
  "$BINARY" mt-pub \
    --engine mqtt \
    --connect "host=$BROKER_HOST" \
    --connect "port=$PUB_BROKER_PORT" \
    --connect "client_id=mt-pub-$run_id" \
    --connect "qos=$qos" \
    --connect "clean_session=false" \
    "${cred_flags[@]}" \
    --topic-prefix "$TOPIC_PREFIX/$run_id" \
    --tenants "$TENANTS" --regions "$REGIONS" --services "$SERVICES" --shards "$SHARDS" \
    --publishers "$PUBLISHERS" \
    --mapping mdim \
    --payload "$PAYLOAD" \
    --rate "$RATE" \
    --duration "$DURATION" \
    "${pub_share_flag[@]}" \
    --csv "$pub_csv" \
    --enable-retry \
    >"$pub_log" 2>&1 &
  local pub_pid=$!

  sleep 1  # Let publisher start

  # Start network failure injection in background
  run_failure_injection_loop "$DURATION" &
  local inject_pid=$!

  # Wait for publisher to finish
  wait "$pub_pid" 2>/dev/null || true

  # Wait for failure injection to finish
  wait "$inject_pid" 2>/dev/null || true

  # Ensure network is clean before subscriber drain phase
  remove_all_toxics

  # Wait for subscriber to finish
  wait "$sub_pid" 2>/dev/null || true

  # Gather metrics
  local sent
  sent=$(parse_last_sent_from_pub_log "$pub_log")
  if [[ "$sent" == "0" ]]; then
    sent=$(csv_last_field "$pub_csv" 2)
  fi

  local received errors dup gaps
  received=$(csv_last_field "$sub_csv" 3)
  errors=$(csv_last_field "$sub_csv" 4)
  dup=$(csv_last_field "$sub_csv" 22)
  gaps=$(csv_last_field "$sub_csv" 23)

  # Count network failures from log
  local net_failures=0
  if [[ -f "${ARTIFACTS_DIR}/network_failures.log" ]]; then
    net_failures=$(grep -c "^[0-9].*,INJECT," "${ARTIFACTS_DIR}/network_failures.log" 2>/dev/null || echo "0")
  fi

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

  # Export results
  _MT_SENT=$sent
  _MT_RECEIVED=$received
  _MT_ERRORS=$errors
  _MT_DUPLICATES=$dup
  _MT_GAPS=$gaps
  _MT_NET_FAILURES=$net_failures
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

  log_section "Testing QoS $qos (multi-topic with network failures)"
  run_single_test "$qos" "$test_id"

  echo "  Sent:              $_MT_SENT"
  echo "  Received:          $_MT_RECEIVED"
  echo "  Errors:            $_MT_ERRORS"
  echo "  Duplicates:        $_MT_DUPLICATES"
  echo "  Gaps:              $_MT_GAPS"
  echo "  Net Failures:      $_MT_NET_FAILURES"
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

  log_section "MQTT multi-topic NETWORK FAILURE test (Toxiproxy)"
  log_info "Artifacts: $ARTIFACTS_DIR"
  log_info "Toxiproxy: $TOXIPROXY_API"
  log_info "  Publisher proxy: $TOXIPROXY_PUB (port $PUB_BROKER_PORT) - clean"
  log_info "  Subscriber proxy: $TOXIPROXY_SUB (port $SUB_BROKER_PORT) - faulted"
  log_info "Keys: T=$TENANTS R=$REGIONS S=$SERVICES K=$SHARDS"
  log_info "QoS levels: $QOS_LEVELS"
  log_info "Failure type: $FAILURE_TYPE, MTTF=${NET_MTTF}s, MTTR=${NET_MTTR}s"
  log_info "payload=$PAYLOAD duration=${DURATION}s rate(per-pub)=${RATE}/s"
  echo ""

  local test_id
  test_id=$(uuidgen)
  log_info "Test ID: $test_id"
  echo ""

  declare -A qos_sent qos_recv qos_err qos_dup qos_gaps qos_netfail qos_loss qos_p50 qos_p95 qos_p99

  for qos in $QOS_LEVELS; do
    run_qos_comparison "$qos" "$test_id"

    qos_sent["$qos"]="$_MT_SENT"
    qos_recv["$qos"]="$_MT_RECEIVED"
    qos_err["$qos"]="$_MT_ERRORS"
    qos_dup["$qos"]="$_MT_DUPLICATES"
    qos_gaps["$qos"]="$_MT_GAPS"
    qos_netfail["$qos"]="$_MT_NET_FAILURES"
    qos_loss["$qos"]="${_MT_LOSS}%"
    qos_p50["$qos"]="${_MT_LAT_P50}ms"
    qos_p95["$qos"]="${_MT_LAT_P95}ms"
    qos_p99["$qos"]="${_MT_LAT_P99}ms"
  done

  # Summary table
  echo "========================================"
  echo "  Multi-topic Network Failure Summary"
  echo "========================================"
  echo ""

  printf "%-18s" "Metric"
  for qos in $QOS_LEVELS; do
    printf " %-12s" "QoS${qos}"
  done
  echo ""
  echo "--------------------------------------------------------------"

  _row() {
    local label="$1"; shift
    local arr_name="$1"; shift
    printf "%-18s" "$label"
    for qos in $QOS_LEVELS; do
      local v
      v=$(eval "echo \${${arr_name}[\"$qos\"]:-0}")
      printf " %-12s" "$v"
    done
    echo ""
  }

  _row "Sent" qos_sent
  _row "Received" qos_recv
  _row "Errors" qos_err
  _row "Duplicates" qos_dup
  _row "Gaps" qos_gaps
  _row "Net Failures" qos_netfail
  _row "Loss" qos_loss
  _row "Latency p50" qos_p50
  _row "Latency p95" qos_p95
  _row "Latency p99" qos_p99
  echo "--------------------------------------------------------------"
  echo ""

  echo "Artifacts saved to: $ARTIFACTS_DIR"
  echo "Network failure log: ${ARTIFACTS_DIR}/network_failures.log"
}

main "$@"
