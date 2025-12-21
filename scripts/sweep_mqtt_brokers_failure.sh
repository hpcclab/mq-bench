#!/usr/bin/env bash
#
# Sweep multi-topic failure test across all MQTT brokers.
#
# Runs test_mqtt_multitopic_failure.sh for each broker defined in docker-compose.yml:
#   - mosquitto (port 1883)
#   - emqx (port 1884)
#   - hivemq (port 1885)
#   - rabbitmq (port 1886)
#   - artemis (port 1887)
#
# Usage:
#   scripts/sweep_mqtt_brokers_failure.sh
#   scripts/sweep_mqtt_brokers_failure.sh --brokers "mosquitto emqx"
#   scripts/sweep_mqtt_brokers_failure.sh --qos "1 2" --duration 60
#   scripts/sweep_mqtt_brokers_failure.sh --host 192.168.0.100
#
# Environment variables from test_mqtt_multitopic_failure.sh are passed through.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ARTIFACTS_BASE="${PROJECT_DIR}/artifacts/sweep_mqtt_failure_$(date +%Y%m%d_%H%M%S)"

# Broker definitions: name:port
declare -A BROKER_PORTS=(
  [mosquitto]=1883
  [emqx]=1884
  [hivemq]=1885
  [rabbitmq]=1886
  [artemis]=1887
)

# Default broker order
DEFAULT_BROKERS="mosquitto emqx hivemq rabbitmq artemis"
BROKERS="${BROKERS:-$DEFAULT_BROKERS}"

# Host (default localhost)
HOST="${HOST:-127.0.0.1}"

# Pass-through parameters for test script
QOS_LEVELS="${QOS_LEVELS:-0 1 2}"
DURATION="${DURATION:-180}"
PAYLOAD="${PAYLOAD:-1024}"
RATE="${RATE:-1}"
TENANTS="${TENANTS:-1}"
REGIONS="${REGIONS:-2}"
SERVICES="${SERVICES:-5}"
SHARDS="${SHARDS:-10}"

# Failure injection defaults
SUB_MTTF="${SUB_MTTF:-30}"
SUB_MTTR="${SUB_MTTR:-5}"
SUB_CRASH_COUNT="${SUB_CRASH_COUNT:-100}"

# Interval between broker tests (seconds)
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

Sweep multi-topic MQTT failure test across all brokers.

Options:
  --brokers "list"      Space-separated broker names (default: all)
                        Available: mosquitto emqx hivemq rabbitmq artemis
  --host HOST           Broker host (default: 127.0.0.1)
  --qos "levels"        QoS levels to test (default: "0 1 2")
  --duration SECS       Test duration per broker (default: 180)
  --payload BYTES       Message payload size (default: 1024)
  --rate N              Messages per second per publisher (default: 1)
  --interval SECS       Pause between brokers (default: 10)
  --mttf SECS           Mean time to failure for subscriber (default: 30)
  --mttr SECS           Mean time to repair (default: 5)
  --crash-count N       Number of crashes to simulate (default: 100)
  -h, --help            Show this help

Environment variables:
  TENANTS, REGIONS, SERVICES, SHARDS - Topic key dimensions
  SUB_CRASH_SEED, PUB_CRASH_LOOP, etc. - See test_mqtt_multitopic_failure.sh

Examples:
  # Test all brokers with defaults
  $(basename "$0")

  # Test only mosquitto and emqx with QoS 1
  $(basename "$0") --brokers "mosquitto emqx" --qos "1"

  # Remote host, shorter duration
  $(basename "$0") --host 192.168.0.100 --duration 60
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --brokers)
      shift; BROKERS="${1:-$DEFAULT_BROKERS}" ;;
    --host)
      shift; HOST="${1:-127.0.0.1}" ;;
    --qos)
      shift; QOS_LEVELS="${1:-0 1 2}" ;;
    --duration)
      shift; DURATION="${1:-180}" ;;
    --payload)
      shift; PAYLOAD="${1:-1024}" ;;
    --rate)
      shift; RATE="${1:-1}" ;;
    --interval)
      shift; INTERVAL_SEC="${1:-10}" ;;
    --mttf)
      shift; SUB_MTTF="${1:-30}" ;;
    --mttr)
      shift; SUB_MTTR="${1:-5}" ;;
    --crash-count)
      shift; SUB_CRASH_COUNT="${1:-100}" ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      log_error "Unknown option: $1"
      usage; exit 1 ;;
  esac
  shift || true
done

# Validate brokers
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

# Check if broker is reachable
check_broker() {
  local host="$1"
  local port="$2"
  if nc -z "$host" "$port" 2>/dev/null; then
    return 0
  else
    return 1
  fi
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
extract_qos_metrics() {
  local broker="$1"
  local broker_artifacts="$2"
  local qos="$3"
  local status="$4"
  local elapsed="$5"
  
  local sub_csv="${broker_artifacts}/mt_sub_qos${qos}.csv"
  local pub_csv="${broker_artifacts}/mt_pub_qos${qos}.csv"
  local pub_log="${broker_artifacts}/mt_pub_qos${qos}.log"
  
  # Default values
  local sent=0 received=0 errors=0 duplicates=0 gaps=0
  local loss_pct="0.0" throughput="0.0"
  local lat_min_ms="0.00" lat_max_ms="0.00" lat_mean_ms="0.00"
  local lat_p50_ms="0.00" lat_p95_ms="0.00" lat_p99_ms="0.00"
  local lat_stdev_ms="0.00"
  local sub_crashes=0 sub_reconnects=0
  
  if [[ -f "$sub_csv" ]]; then
    sent=$(parse_last_sent_from_pub_log "$pub_log")
    if [[ "$sent" == "0" ]] && [[ -f "$pub_csv" ]]; then
      sent=$(csv_last_field "$pub_csv" 2)
    fi
    
    # Sub CSV columns:
    # 3:received, 4:errors, 5:throughput
    # 6:lat_min, 7:lat_p50, 8:lat_p95, 9:lat_p99, 10:lat_max, 11:lat_mean
    # 13:lat_stdev, 19:crashes, 20:reconnects
    # 22:duplicates, 23:gaps
    
    received=$(csv_last_field "$sub_csv" 3)
    errors=$(csv_last_field "$sub_csv" 4)
    throughput=$(csv_last_field "$sub_csv" 5)
    duplicates=$(csv_last_field "$sub_csv" 22)
    gaps=$(csv_last_field "$sub_csv" 23)
    sub_crashes=$(csv_last_field "$sub_csv" 19)
    sub_reconnects=$(csv_last_field "$sub_csv" 20)
    
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
    
    if [[ "$sent" -gt 0 ]]; then
      loss_pct=$(awk -v s="$sent" -v r="$received" 'BEGIN{ printf("%.2f", (s - r) * 100.0 / s) }')
    fi
  fi
  
  # Write row: broker,status,qos,duration_sec,sent,received,loss_pct,errors,duplicates,gaps,crashes,reconnects,throughput,lat_min_ms,lat_mean_ms,lat_p50_ms,lat_p95_ms,lat_p99_ms,lat_max_ms,lat_stdev_ms,mttf,mttr,payload,rate
  echo "${broker},${status},${qos},${elapsed},${sent},${received},${loss_pct},${errors},${duplicates},${gaps},${sub_crashes},${sub_reconnects},${throughput},${lat_min_ms},${lat_mean_ms},${lat_p50_ms},${lat_p95_ms},${lat_p99_ms},${lat_max_ms},${lat_stdev_ms},${SUB_MTTF},${SUB_MTTR},${PAYLOAD},${RATE}" >> "${ARTIFACTS_BASE}/summary.csv"
}

# Run test for a single broker
run_broker_test() {
  local broker="$1"
  local port="${BROKER_PORTS[$broker]}"
  local broker_artifacts="${ARTIFACTS_BASE}/${broker}"
  
  log_section "Testing broker: $broker ($HOST:$port)"
  
  if ! check_broker "$HOST" "$port"; then
    log_warn "Broker $broker not reachable on $HOST:$port, skipping"
    for qos in $QOS_LEVELS; do
      echo "${broker},SKIP,${qos},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,${SUB_MTTF},${SUB_MTTR},${PAYLOAD},${RATE}" >> "${ARTIFACTS_BASE}/summary.csv"
    done
    return 0
  fi
  
  mkdir -p "$broker_artifacts"
  
  # Export environment for the test script
  export BROKER_HOST="$HOST"
  export BROKER_PORT="$port"
  export QOS_LEVELS="$QOS_LEVELS"
  export DURATION="$DURATION"
  export PAYLOAD="$PAYLOAD"
  export RATE="$RATE"
  export TENANTS="$TENANTS"
  export REGIONS="$REGIONS"
  export SERVICES="$SERVICES"
  export SHARDS="$SHARDS"
  export SUB_MTTF="$SUB_MTTF"
  export SUB_MTTR="$SUB_MTTR"
  export SUB_CRASH_COUNT="$SUB_CRASH_COUNT"
  
  # Broker-specific credentials
  if [[ "$broker" == "artemis" ]]; then
    export MQTT_USERNAME="${ARTEMIS_USERNAME:-admin}"
    export MQTT_PASSWORD="${ARTEMIS_PASSWORD:-admin}"
  else
    # Clear credentials for other brokers (they use anonymous)
    unset MQTT_USERNAME MQTT_PASSWORD 2>/dev/null || true
  fi
  
  # Override artifacts dir to be under our sweep directory
  export ARTIFACTS_DIR="$broker_artifacts"
  
  local test_log="${broker_artifacts}/test.log"
  local start_ts=$(date +%s)
  
  # Run the test
  if bash "${SCRIPT_DIR}/test_mqtt_multitopic_failure.sh" > "$test_log" 2>&1; then
    local end_ts=$(date +%s)
    local elapsed=$((end_ts - start_ts))
    log_info "Broker $broker completed in ${elapsed}s"
    
    # Extract detailed metrics for each QoS level
    for qos in $QOS_LEVELS; do
      extract_qos_metrics "$broker" "$broker_artifacts" "$qos" "OK" "$elapsed"
    done
  else
    local end_ts=$(date +%s)
    local elapsed=$((end_ts - start_ts))
    log_error "Broker $broker failed after ${elapsed}s (see $test_log)"
    
    # Write failure row for each QoS level
    for qos in $QOS_LEVELS; do
      echo "${broker},FAIL,${qos},${elapsed},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,${SUB_MTTF},${SUB_MTTR},${PAYLOAD},${RATE}" >> "${ARTIFACTS_BASE}/summary.csv"
    done
  fi
}

main() {
  validate_brokers
  
  mkdir -p "$ARTIFACTS_BASE"
  
  log_section "MQTT Broker Failure Test Sweep"
  log_info "Artifacts: $ARTIFACTS_BASE"
  log_info "Brokers: $BROKERS"
  log_info "Host: $HOST"
  log_info "QoS levels: $QOS_LEVELS"
  log_info "Duration: ${DURATION}s, Payload: ${PAYLOAD}B, Rate: ${RATE}/s"
  log_info "Crash params: MTTF=${SUB_MTTF}s, MTTR=${SUB_MTTR}s, count=${SUB_CRASH_COUNT}"
  echo ""
  
  # Write summary CSV header
  echo "broker,status,qos,duration_sec,sent,received,loss_pct,errors,duplicates,gaps,crashes,reconnects,throughput,lat_min_ms,lat_mean_ms,lat_p50_ms,lat_p95_ms,lat_p99_ms,lat_max_ms,lat_stdev_ms,mttf,mttr,payload,rate" > "${ARTIFACTS_BASE}/summary.csv"
  
  local broker_count=0
  local total_brokers
  total_brokers=$(echo "$BROKERS" | wc -w)
  
  for broker in $BROKERS; do
    broker_count=$((broker_count + 1))
    log_info "[$broker_count/$total_brokers] Starting $broker..."
    
    run_broker_test "$broker"
    
    # Pause between brokers (except after last one)
    if [[ $broker_count -lt $total_brokers ]] && [[ $INTERVAL_SEC -gt 0 ]]; then
      log_info "Waiting ${INTERVAL_SEC}s before next broker..."
      sleep "$INTERVAL_SEC"
    fi
  done
  
  echo ""
  log_section "Sweep Complete"
  echo ""
  
  # Print summary table (grouped by broker and QoS)
  echo "============================================================================================="
  echo "                         Broker Client-Crash Sweep Summary"
  echo "============================================================================================="
  echo ""
  printf "%-10s %-4s %-6s %-10s %-10s %-8s %-8s %-8s %-8s %-8s\n" \
    "Broker" "QoS" "Status" "Sent" "Received" "Loss%" "Gaps" "Crashes" "P50ms" "P99ms"
  echo "---------------------------------------------------------------------------------------------"
  tail -n +2 "${ARTIFACTS_BASE}/summary.csv" | while IFS=, read -r broker status qos dur sent recv loss err dup gaps crashes reconn tput lat_min lat_mean lat_p50 lat_p95 lat_p99 lat_max lat_std mttf mttr payload rate; do
    printf "%-10s %-4s %-6s %-10s %-10s %-8s %-8s %-8s %-8s %-8s\n" \
      "$broker" "$qos" "$status" "$sent" "$recv" "$loss" "$gaps" "$crashes" "$lat_p50" "$lat_p99"
  done
  echo "---------------------------------------------------------------------------------------------"
  echo ""
  
  # Print test configuration
  echo "Test Configuration:"
  echo "  Crash Type: Client process crash (force_disconnect)"
  echo "  MTTF: ${SUB_MTTF}s, MTTR: ${SUB_MTTR}s, Max Crashes: $SUB_CRASH_COUNT"
  echo "  Duration: ${DURATION}s, Payload: ${PAYLOAD}B, Rate: ${RATE}/s"
  echo ""
  echo "Detailed results: $ARTIFACTS_BASE"
  echo "Summary CSV: ${ARTIFACTS_BASE}/summary.csv"
}

main "$@"
