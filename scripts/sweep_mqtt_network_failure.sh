#!/usr/bin/env bash
#
# Sweep multi-topic NETWORK FAILURE test across all MQTT brokers using Toxiproxy.
#
# This script:
# 1. Ensures Toxiproxy is running
# 2. Creates/updates proxies for each broker (separate pub/sub proxies)
# 3. Runs network failure tests using Toxiproxy to inject failures
#
# KEY DESIGN: Separate proxies for publisher and subscriber
#   - Publisher uses port 21xxx (clean proxy, no faults)
#   - Subscriber uses port 22xxx (faulted proxy)
#
# Unlike process crashes, Toxiproxy provides REAL network failures:
#   - reset_peer: TCP RST (abrupt disconnect)
#   - timeout: Network partition (blackhole)
#   - latency: Network delay
#   - limit_data: Bandwidth limit
#
# Prereqs:
#   - MQTT brokers running (docker compose up -d)
#   - cargo build --release
#
# Usage:
#   scripts/sweep_mqtt_network_failure.sh
#   scripts/sweep_mqtt_network_failure.sh --brokers "mosquitto emqx"
#   scripts/sweep_mqtt_network_failure.sh --failure-type timeout
#   FAILURE_TYPE=latency LATENCY_MS=1000 scripts/sweep_mqtt_network_failure.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ARTIFACTS_BASE="${PROJECT_DIR}/artifacts/sweep_mqtt_netfail_$(date +%Y%m%d_%H%M%S)"

# Toxiproxy settings
TOXIPROXY_API="${TOXIPROXY_API:-http://127.0.0.1:8474}"
TOXIPROXY_CONTAINER="${TOXIPROXY_CONTAINER:-toxiproxy}"

# Broker definitions: name -> direct_port
declare -A BROKER_PORTS=(
  [mosquitto]=1883
  [emqx]=1884
  [hivemq]=1885
  [rabbitmq]=1886
  [artemis]=1887
)

# Publisher proxy ports (clean, no faults) - 21xxx
declare -A PUB_PROXY_PORTS=(
  [mosquitto]=21883
  [emqx]=21884
  [hivemq]=21885
  [rabbitmq]=21886
  [artemis]=21887
)

# Subscriber proxy ports (faulted) - 22xxx
declare -A SUB_PROXY_PORTS=(
  [mosquitto]=22883
  [emqx]=22884
  [hivemq]=22885
  [rabbitmq]=22886
  [artemis]=22887
)

# Proxy names
declare -A PUB_PROXY_NAMES=(
  [mosquitto]=mqtt_mosquitto_pub
  [emqx]=mqtt_emqx_pub
  [hivemq]=mqtt_hivemq_pub
  [rabbitmq]=mqtt_rabbitmq_pub
  [artemis]=mqtt_artemis_pub
)

declare -A SUB_PROXY_NAMES=(
  [mosquitto]=mqtt_mosquitto_sub
  [emqx]=mqtt_emqx_sub
  [hivemq]=mqtt_hivemq_sub
  [rabbitmq]=mqtt_rabbitmq_sub
  [artemis]=mqtt_artemis_sub
)

DEFAULT_BROKERS="mosquitto emqx hivemq rabbitmq artemis"
BROKERS="${BROKERS:-$DEFAULT_BROKERS}"

HOST="${HOST:-127.0.0.1}"

# Test parameters
QOS_LEVELS="${QOS_LEVELS:-0 1 2}"
DURATION="${DURATION:-180}"
PAYLOAD="${PAYLOAD:-1024}"
RATE="${RATE:-1}"
TENANTS="${TENANTS:-1}"
REGIONS="${REGIONS:-2}"
SERVICES="${SERVICES:-5}"
SHARDS="${SHARDS:-10}"

# Network failure parameters
FAILURE_TYPE="${FAILURE_TYPE:-reset_peer}"
NET_MTTF="${NET_MTTF:-30}"
NET_MTTR="${NET_MTTR:-5}"
NET_FAILURE_COUNT="${NET_FAILURE_COUNT:-100}"
LATENCY_MS="${LATENCY_MS:-500}"
LATENCY_JITTER_MS="${LATENCY_JITTER_MS:-100}"

# Interval between broker tests
INTERVAL_SEC="${INTERVAL_SEC:-10}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_section() { echo -e "${CYAN}${BOLD}[====]${NC} $*"; }

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Sweep multi-topic MQTT network failure test across all brokers using Toxiproxy.

Options:
  --brokers "list"        Space-separated broker names (default: all)
                          Available: mosquitto emqx hivemq rabbitmq artemis
  --host HOST             Broker host (default: 127.0.0.1)
  --qos "levels"          QoS levels to test (default: "0 1 2")
  --duration SECS         Test duration per broker (default: 180)
  --payload BYTES         Message payload size (default: 1024)
  --rate N                Messages per second per publisher (default: 1)
  --interval SECS         Pause between brokers (default: 10)
  
Network Failure Options:
  --failure-type TYPE     Type of network failure to inject:
                            reset_peer - TCP connection reset (default)
                            timeout    - Network partition (blackhole)
                            latency    - Add network delay
                            limit_data - Bandwidth limit then close
  --mttf SECS             Mean time to failure (default: 30)
  --mttr SECS             Mean time to repair (default: 5)
  --failure-count N       Number of failures to inject (default: 100)
  --latency MS            Latency in ms (for latency type, default: 500)
  --jitter MS             Latency jitter in ms (default: 100)

  -h, --help              Show this help

Environment variables:
  TOXIPROXY_API         - Toxiproxy API endpoint (default: http://127.0.0.1:8474)
  TENANTS, REGIONS, SERVICES, SHARDS - Topic key dimensions

Examples:
  # Test all brokers with TCP reset failures
  $(basename "$0")

  # Test with network partition (timeout)
  $(basename "$0") --failure-type timeout

  # Test with latency injection
  $(basename "$0") --failure-type latency --latency 1000 --jitter 200

  # Test specific brokers
  $(basename "$0") --brokers "mosquitto emqx" --qos "1"
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --brokers)       shift; BROKERS="${1:-$DEFAULT_BROKERS}" ;;
    --host)          shift; HOST="${1:-127.0.0.1}" ;;
    --qos)           shift; QOS_LEVELS="${1:-0 1 2}" ;;
    --duration)      shift; DURATION="${1:-180}" ;;
    --payload)       shift; PAYLOAD="${1:-1024}" ;;
    --rate)          shift; RATE="${1:-1}" ;;
    --interval)      shift; INTERVAL_SEC="${1:-10}" ;;
    --failure-type)  shift; FAILURE_TYPE="${1:-reset_peer}" ;;
    --mttf)          shift; NET_MTTF="${1:-30}" ;;
    --mttr)          shift; NET_MTTR="${1:-5}" ;;
    --failure-count) shift; NET_FAILURE_COUNT="${1:-100}" ;;
    --latency)       shift; LATENCY_MS="${1:-500}" ;;
    --jitter)        shift; LATENCY_JITTER_MS="${1:-100}" ;;
    -h|--help)       usage; exit 0 ;;
    *)
      log_error "Unknown option: $1"
      usage; exit 1 ;;
  esac
  shift || true
done

#=============================================================================
# Toxiproxy management
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

is_toxiproxy_running() {
  curl -s "${TOXIPROXY_API}/version" >/dev/null 2>&1
}

start_toxiproxy() {
  log_info "Starting Toxiproxy container..."
  
  # Check if container exists but stopped
  if docker ps -a --format '{{.Names}}' | grep -q "^${TOXIPROXY_CONTAINER}$"; then
    docker rm -f "$TOXIPROXY_CONTAINER" >/dev/null 2>&1 || true
  fi
  
  # Start toxiproxy with host networking
  docker run -d \
    --name "$TOXIPROXY_CONTAINER" \
    --net=host \
    ghcr.io/shopify/toxiproxy \
    >/dev/null 2>&1
  
  # Wait for it to be ready
  local retries=10
  while [[ $retries -gt 0 ]]; do
    if is_toxiproxy_running; then
      log_info "Toxiproxy started successfully"
      return 0
    fi
    sleep 0.5
    retries=$((retries - 1))
  done
  
  log_error "Failed to start Toxiproxy"
  return 1
}

ensure_toxiproxy() {
  if is_toxiproxy_running; then
    log_info "Toxiproxy already running at $TOXIPROXY_API"
    return 0
  fi
  
  start_toxiproxy
}

# Create or update a proxy
create_proxy() {
  local name="$1"
  local listen_port="$2"
  local upstream_port="$3"
  
  # Check if proxy exists
  local existing
  existing=$(toxiproxy_api GET "/proxies/${name}" 2>/dev/null || echo "")
  
  if [[ -n "$existing" ]] && echo "$existing" | grep -q "\"name\""; then
    # Proxy exists, ensure it's enabled and remove toxics
    toxiproxy_api POST "/proxies/${name}" '{"enabled": true}' >/dev/null 2>&1 || true
    # Remove all toxics
    local toxics
    toxics=$(toxiproxy_api GET "/proxies/${name}/toxics" 2>/dev/null || echo "[]")
    if [[ "$toxics" != "[]" ]]; then
      echo "$toxics" | jq -r '.[].name' 2>/dev/null | while read -r tname; do
        toxiproxy_api DELETE "/proxies/${name}/toxics/${tname}" >/dev/null 2>&1 || true
      done
    fi
    log_info "Proxy $name already exists, cleaned up"
    return 0
  fi
  
  # Create new proxy
  local payload
  payload=$(cat <<EOF
{
  "name": "$name",
  "listen": "0.0.0.0:$listen_port",
  "upstream": "${HOST}:${upstream_port}",
  "enabled": true
}
EOF
)
  
  if toxiproxy_api POST "/proxies" "$payload" >/dev/null 2>&1; then
    log_info "Created proxy: $name (localhost:$listen_port -> $HOST:$upstream_port)"
    return 0
  else
    log_error "Failed to create proxy: $name"
    return 1
  fi
}

# Setup all proxies for the brokers we'll test
# Creates BOTH pub (clean) and sub (faulted) proxies for each broker
setup_proxies() {
  log_info "Setting up Toxiproxy proxies (pub=clean, sub=faulted)..."
  
  for broker in $BROKERS; do
    if [[ ! -v "BROKER_PORTS[$broker]" ]]; then
      continue
    fi
    
    local direct_port="${BROKER_PORTS[$broker]}"
    local pub_proxy_port="${PUB_PROXY_PORTS[$broker]}"
    local sub_proxy_port="${SUB_PROXY_PORTS[$broker]}"
    local pub_proxy_name="${PUB_PROXY_NAMES[$broker]}"
    local sub_proxy_name="${SUB_PROXY_NAMES[$broker]}"
    
    # Check if broker is reachable first
    if ! nc -z "$HOST" "$direct_port" 2>/dev/null; then
      log_warn "Broker $broker not reachable on $HOST:$direct_port, skipping proxy"
      continue
    fi
    
    # Create publisher proxy (clean, no faults)
    create_proxy "$pub_proxy_name" "$pub_proxy_port" "$direct_port"
    # Create subscriber proxy (will have faults injected)
    create_proxy "$sub_proxy_name" "$sub_proxy_port" "$direct_port"
  done
}

#=============================================================================
# Validation
#=============================================================================

validate_brokers() {
  local valid_brokers=""
  for b in $BROKERS; do
    if [[ -v "BROKER_PORTS[$b]" ]]; then
      valid_brokers="$valid_brokers $b"
    else
      log_warn "Unknown broker '$b', skipping. Available: ${!BROKER_PORTS[*]}"
    fi
  done
  BROKERS="${valid_brokers# }"
  if [[ -z "$BROKERS" ]]; then
    log_error "No valid brokers specified"
    exit 1
  fi
}

validate_failure_type() {
  case "$FAILURE_TYPE" in
    reset_peer|timeout|latency|limit_data)
      return 0
      ;;
    *)
      log_error "Invalid failure type: $FAILURE_TYPE"
      log_info "Available: reset_peer, timeout, latency, limit_data"
      exit 1
      ;;
  esac
}

check_broker() {
  local host="$1"
  local port="$2"
  nc -z "$host" "$port" 2>/dev/null
}

#=============================================================================
# Metrics extraction
#=============================================================================

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

# Extract metrics for a specific QoS level from broker artifacts
# Writes one row per QoS to the summary CSV
extract_qos_metrics() {
  local broker="$1"
  local broker_artifacts="$2"
  local qos="$3"
  local status="$4"
  local elapsed="$5"
  local net_failures="$6"
  
  local sub_csv="${broker_artifacts}/mt_sub_qos${qos}.csv"
  local pub_csv="${broker_artifacts}/mt_pub_qos${qos}.csv"
  local pub_log="${broker_artifacts}/mt_pub_qos${qos}.log"
  
  # Default values
  local sent=0 received=0 errors=0 duplicates=0 gaps=0
  local loss_pct="0.0" throughput="0.0"
  local lat_min_ms="0.00" lat_max_ms="0.00" lat_mean_ms="0.00"
  local lat_p50_ms="0.00" lat_p95_ms="0.00" lat_p99_ms="0.00"
  local lat_stdev_ms="0.00"
  
  if [[ -f "$sub_csv" ]]; then
    # Parse sent from log or CSV
    sent=$(parse_last_sent_from_pub_log "$pub_log")
    if [[ "$sent" == "0" ]] && [[ -f "$pub_csv" ]]; then
      sent=$(csv_last_field "$pub_csv" 2)
    fi
    
    # Sub CSV columns (based on stats.rs output):
    # 1:timestamp, 2:elapsed, 3:received, 4:errors, 5:throughput
    # 6:lat_min, 7:lat_p50, 8:lat_p95, 9:lat_p99, 10:lat_max, 11:lat_mean
    # 12:lat_variance, 13:lat_stdev, 14:interval_stdev
    # ...more columns...
    # 22:duplicates, 23:gaps
    
    received=$(csv_last_field "$sub_csv" 3)
    errors=$(csv_last_field "$sub_csv" 4)
    throughput=$(csv_last_field "$sub_csv" 5)
    duplicates=$(csv_last_field "$sub_csv" 22)
    gaps=$(csv_last_field "$sub_csv" 23)
    
    # Latency (in nanoseconds in CSV, convert to ms)
    local lat_min_ns lat_max_ns lat_mean_ns lat_p50_ns lat_p95_ns lat_p99_ns lat_stdev_ns
    lat_min_ns=$(csv_last_field "$sub_csv" 6)
    lat_p50_ns=$(csv_last_field "$sub_csv" 7)
    lat_p95_ns=$(csv_last_field "$sub_csv" 8)
    lat_p99_ns=$(csv_last_field "$sub_csv" 9)
    lat_max_ns=$(csv_last_field "$sub_csv" 10)
    lat_mean_ns=$(csv_last_field "$sub_csv" 11)
    lat_stdev_ns=$(csv_last_field "$sub_csv" 13)
    
    lat_min_ms=$(ns_to_ms "$lat_min_ns")
    lat_max_ms=$(ns_to_ms "$lat_max_ns")
    lat_mean_ms=$(ns_to_ms "$lat_mean_ns")
    lat_p50_ms=$(ns_to_ms "$lat_p50_ns")
    lat_p95_ms=$(ns_to_ms "$lat_p95_ns")
    lat_p99_ms=$(ns_to_ms "$lat_p99_ns")
    lat_stdev_ms=$(ns_to_ms "$lat_stdev_ns")
    
    # Calculate loss percentage
    if [[ "$sent" -gt 0 ]]; then
      loss_pct=$(awk -v s="$sent" -v r="$received" 'BEGIN{ printf("%.2f", (s - r) * 100.0 / s) }')
    fi
  fi
  
  # Write row to summary CSV
  # broker,status,qos,duration_sec,failure_type,net_failures,sent,received,loss_pct,errors,duplicates,gaps,throughput,lat_min_ms,lat_mean_ms,lat_p50_ms,lat_p95_ms,lat_p99_ms,lat_max_ms,lat_stdev_ms,mttf,mttr,payload,rate
  echo "${broker},${status},${qos},${elapsed},${FAILURE_TYPE},${net_failures},${sent},${received},${loss_pct},${errors},${duplicates},${gaps},${throughput},${lat_min_ms},${lat_mean_ms},${lat_p50_ms},${lat_p95_ms},${lat_p99_ms},${lat_max_ms},${lat_stdev_ms},${NET_MTTF},${NET_MTTR},${PAYLOAD},${RATE}" >> "${ARTIFACTS_BASE}/summary.csv"
}

#=============================================================================
# Test execution
#=============================================================================

run_broker_test() {
  local broker="$1"
  local direct_port="${BROKER_PORTS[$broker]}"
  local pub_proxy_port="${PUB_PROXY_PORTS[$broker]}"
  local sub_proxy_port="${SUB_PROXY_PORTS[$broker]}"
  local pub_proxy_name="${PUB_PROXY_NAMES[$broker]}"
  local sub_proxy_name="${SUB_PROXY_NAMES[$broker]}"
  local broker_artifacts="${ARTIFACTS_BASE}/${broker}"
  
  log_section "Testing broker: $broker (pub:$pub_proxy_port sub:$sub_proxy_port)"
  
  # Check broker is reachable directly
  if ! check_broker "$HOST" "$direct_port"; then
    log_warn "Broker $broker not reachable on $HOST:$direct_port, skipping"
    for qos in $QOS_LEVELS; do
      echo "${broker},SKIP,${qos},0,${FAILURE_TYPE},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,${NET_MTTF},${NET_MTTR},${PAYLOAD},${RATE}" >> "${ARTIFACTS_BASE}/summary.csv"
    done
    return 0
  fi
  
  # Check both proxies are working
  if ! check_broker "127.0.0.1" "$pub_proxy_port"; then
    log_warn "Pub proxy for $broker not reachable on 127.0.0.1:$pub_proxy_port, skipping"
    for qos in $QOS_LEVELS; do
      echo "${broker},SKIP,${qos},0,${FAILURE_TYPE},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,${NET_MTTF},${NET_MTTR},${PAYLOAD},${RATE}" >> "${ARTIFACTS_BASE}/summary.csv"
    done
    return 0
  fi
  
  if ! check_broker "127.0.0.1" "$sub_proxy_port"; then
    log_warn "Sub proxy for $broker not reachable on 127.0.0.1:$sub_proxy_port, skipping"
    for qos in $QOS_LEVELS; do
      echo "${broker},SKIP,${qos},0,${FAILURE_TYPE},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,${NET_MTTF},${NET_MTTR},${PAYLOAD},${RATE}" >> "${ARTIFACTS_BASE}/summary.csv"
    done
    return 0
  fi
  
  mkdir -p "$broker_artifacts"
  
  # Export environment for the test script
  # Separate proxies: publisher uses clean proxy, subscriber uses faulted proxy
  export BROKER_HOST="127.0.0.1"
  export PUB_BROKER_PORT="$pub_proxy_port"   # Clean proxy for publisher
  export SUB_BROKER_PORT="$sub_proxy_port"   # Faulted proxy for subscriber
  export TOXIPROXY_API="$TOXIPROXY_API"
  export TOXIPROXY_PUB="$pub_proxy_name"
  export TOXIPROXY_SUB="$sub_proxy_name"
  
  export QOS_LEVELS="$QOS_LEVELS"
  export DURATION="$DURATION"
  export PAYLOAD="$PAYLOAD"
  export RATE="$RATE"
  export TENANTS="$TENANTS"
  export REGIONS="$REGIONS"
  export SERVICES="$SERVICES"
  export SHARDS="$SHARDS"
  
  export FAILURE_TYPE="$FAILURE_TYPE"
  export NET_MTTF="$NET_MTTF"
  export NET_MTTR="$NET_MTTR"
  export NET_FAILURE_COUNT="$NET_FAILURE_COUNT"
  export LATENCY_MS="$LATENCY_MS"
  export LATENCY_JITTER_MS="$LATENCY_JITTER_MS"
  
  # Broker-specific credentials
  if [[ "$broker" == "artemis" ]]; then
    export MQTT_USERNAME="${ARTEMIS_USERNAME:-admin}"
    export MQTT_PASSWORD="${ARTEMIS_PASSWORD:-admin}"
  else
    unset MQTT_USERNAME MQTT_PASSWORD 2>/dev/null || true
  fi
  
  export ARTIFACTS_DIR="$broker_artifacts"
  
  local test_log="${broker_artifacts}/test.log"
  local start_ts=$(date +%s)
  
  if bash "${SCRIPT_DIR}/test_mqtt_network_failure.sh" > "$test_log" 2>&1; then
    local end_ts=$(date +%s)
    local elapsed=$((end_ts - start_ts))
    
    # Count network failures from log
    local net_failures=0
    if [[ -f "${broker_artifacts}/network_failures.log" ]]; then
      net_failures=$(grep -c "^[0-9].*,INJECT," "${broker_artifacts}/network_failures.log" 2>/dev/null || echo "0")
    fi
    
    log_info "Broker $broker completed in ${elapsed}s ($net_failures network failures)"
    
    # Extract detailed metrics for each QoS level
    for qos in $QOS_LEVELS; do
      extract_qos_metrics "$broker" "$broker_artifacts" "$qos" "OK" "$elapsed" "$net_failures"
    done
  else
    local end_ts=$(date +%s)
    local elapsed=$((end_ts - start_ts))
    log_error "Broker $broker failed after ${elapsed}s (see $test_log)"
    
    # Write failure row for each QoS level
    for qos in $QOS_LEVELS; do
      echo "${broker},FAIL,${qos},${elapsed},${FAILURE_TYPE},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,${NET_MTTF},${NET_MTTR},${PAYLOAD},${RATE}" >> "${ARTIFACTS_BASE}/summary.csv"
    done
  fi
}

#=============================================================================
# Main
#=============================================================================

main() {
  validate_brokers
  validate_failure_type
  
  mkdir -p "$ARTIFACTS_BASE"
  
  log_section "MQTT Broker Network Failure Test Sweep (Toxiproxy)"
  log_info "Artifacts: $ARTIFACTS_BASE"
  log_info "Brokers: $BROKERS"
  log_info "Host: $HOST"
  log_info "QoS levels: $QOS_LEVELS"
  log_info "Duration: ${DURATION}s, Payload: ${PAYLOAD}B, Rate: ${RATE}/s"
  log_info "Failure type: $FAILURE_TYPE"
  log_info "Failure params: MTTF=${NET_MTTF}s, MTTR=${NET_MTTR}s, count=${NET_FAILURE_COUNT}"
  if [[ "$FAILURE_TYPE" == "latency" ]]; then
    log_info "Latency: ${LATENCY_MS}ms ± ${LATENCY_JITTER_MS}ms"
  fi
  echo ""
  
  # Ensure Toxiproxy is running
  ensure_toxiproxy
  
  # Setup proxies for all brokers
  setup_proxies
  
  echo ""
  
  # Write summary CSV header
  echo "broker,status,qos,duration_sec,failure_type,net_failures,sent,received,loss_pct,errors,duplicates,gaps,throughput,lat_min_ms,lat_mean_ms,lat_p50_ms,lat_p95_ms,lat_p99_ms,lat_max_ms,lat_stdev_ms,mttf,mttr,payload,rate" > "${ARTIFACTS_BASE}/summary.csv"
  
  local broker_count=0
  local total_brokers
  total_brokers=$(echo "$BROKERS" | wc -w)
  
  for broker in $BROKERS; do
    broker_count=$((broker_count + 1))
    log_info "[$broker_count/$total_brokers] Starting $broker..."
    
    run_broker_test "$broker"
    
    if [[ $broker_count -lt $total_brokers ]] && [[ $INTERVAL_SEC -gt 0 ]]; then
      log_info "Waiting ${INTERVAL_SEC}s before next broker..."
      sleep "$INTERVAL_SEC"
    fi
  done
  
  echo ""
  log_section "Sweep Complete"
  echo ""
  
  # Print summary table (grouped by broker)
  echo "============================================================================================="
  echo "                        Broker Network Failure Sweep Summary"
  echo "============================================================================================="
  echo ""
  printf "%-10s %-4s %-6s %-10s %-10s %-8s %-8s %-8s %-8s %-8s\n" \
    "Broker" "QoS" "Status" "Sent" "Received" "Loss%" "Gaps" "NetFail" "P50ms" "P99ms"
  echo "---------------------------------------------------------------------------------------------"
  tail -n +2 "${ARTIFACTS_BASE}/summary.csv" | while IFS=, read -r broker status qos dur ftype nf sent recv loss err dup gaps tput lat_min lat_mean lat_p50 lat_p95 lat_p99 lat_max lat_std mttf mttr payload rate; do
    printf "%-10s %-4s %-6s %-10s %-10s %-8s %-8s %-8s %-8s %-8s\n" \
      "$broker" "$qos" "$status" "$sent" "$recv" "$loss" "$gaps" "$nf" "$lat_p50" "$lat_p99"
  done
  echo "---------------------------------------------------------------------------------------------"
  echo ""
  
  # Print test configuration
  echo "Test Configuration:"
  echo "  Failure Type: $FAILURE_TYPE"
  echo "  MTTF: ${NET_MTTF}s, MTTR: ${NET_MTTR}s, Max Failures: $NET_FAILURE_COUNT"
  echo "  Duration: ${DURATION}s, Payload: ${PAYLOAD}B, Rate: ${RATE}/s"
  if [[ "$FAILURE_TYPE" == "latency" ]]; then
    echo "  Latency: ${LATENCY_MS}ms ± ${LATENCY_JITTER_MS}ms"
  fi
  echo ""
  echo "Detailed results: $ARTIFACTS_BASE"
  echo "Summary CSV: ${ARTIFACTS_BASE}/summary.csv"
}

main "$@"
