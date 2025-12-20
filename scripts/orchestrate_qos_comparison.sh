#!/usr/bin/env bash
set -euo pipefail

# Orchestrate QoS comparison experiment across MQTT brokers.
#
# Purpose: Compare latency across QoS 0/1/2 for MQTT brokers only.
# Output: Horizontal bar chart showing P50/P95/P99 latency per broker × QoS level.
#
# Topology: N publishers → N subscribers (1:1 pair per topic)
# Workload: Fixed payload, fixed rate, sweep QoS levels per broker.
# Runner: uses scripts/run_multi_topic_perkey.sh
# Output: results/qos_comparison_<ts>/{raw_data,plots}/ with summary.csv and figures
#
# Usage examples:
#   # Default: 5 MQTT brokers, QoS 0/1/2, payload=1KB, pairs=10, rate=100 msg/s
#   scripts/orchestrate_qos_comparison.sh
#
#   # Remote host for all brokers
#   scripts/orchestrate_qos_comparison.sh --host 192.168.0.254
#
#   # Sequential mode: start/stop containers one by one
#   scripts/orchestrate_qos_comparison.sh --sequential
#
#   # Customize brokers and QoS levels
#   scripts/orchestrate_qos_comparison.sh \
#     --mqtt-brokers "mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884" \
#     --qos-levels "0 1 2"
#
#   # Higher rate test (warning: QoS 2 may struggle)
#   scripts/orchestrate_qos_comparison.sh --total-rate 500 --pairs 10

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Source lib.sh for helper functions
source "${SCRIPT_DIR}/lib.sh"

# Defaults - conservative for fair QoS comparison
PAIRS=10                      # N pub/sub pairs
TOTAL_RATE=100                # msgs/s total (low for QoS 2 fairness)
PAYLOAD=1024                  # 1KB payload
DURATION=30                   # seconds
SNAPSHOT=5
RUN_ID_PREFIX="qos"
QOS_LEVELS=(0 1 2)            # MQTT QoS levels to test
DRY_RUN=${DRY_RUN:-0}
HOST="${HOST:-}"
SUMMARY_OVERRIDE="${SUMMARY_OVERRIDE:-}"
INTERVAL_SEC=15               # seconds between tests (TIME_WAIT cleanup)

# Fairness features (from throughput script)
WARMUP_SECS=5                 # warmup duration per broker
WARMUP_PAYLOAD=1024           # warmup payload size
IGNORE_START_SECS=5           # ignore ramp-up period
IGNORE_END_SECS=5             # ignore ramp-down period
RAMP_UP_SECS=5                # subscriber ramp-up

# Sequential / Remote execution
SEQUENTIAL=0
SSH_TARGET=""
REMOTE_DIR="~/mq-bench"
START_SERVICES=0

# MQTT brokers (name:host:port). Default to local compose ports.
# Only MQTT brokers - they all support QoS 0/1/2
DEFAULT_MQTT_BROKERS="mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885 rabbitmq:127.0.0.1:1886 artemis:127.0.0.1:1887"
MQTT_BROKERS="${DEFAULT_MQTT_BROKERS}"
declare -a MQTT_BROKERS_ARR=()

# Helper to clean smart quotes from a string
clean_quotes() {
  local v="$1"
  v="${v#[\"\'""'']}"
  v="${v%[\"\'""'']}"
  echo "$v"
}

is_quote_only_token() {
  local v="$1"
  case "$v" in
    "\"\""|"''"|""""|""""|"''"|"''") return 0;;
    *) return 1;;
  esac
}

timestamp() { date +%Y%m%d_%H%M%S; }

# Directories
TS=""
BENCH_DIR=""
RAW_DIR=""
PLOTS_DIR=""
SUMMARY_CSV=""

init_dirs() {
  if [[ -z "${TS}" ]]; then TS="$(timestamp)"; fi
  if [[ -z "${BENCH_DIR}" ]]; then BENCH_DIR="${REPO_ROOT}/results/qos_comparison_${TS}"; fi
  if [[ -z "${RAW_DIR}" ]]; then RAW_DIR="${BENCH_DIR}/raw_data"; fi
  if [[ -z "${PLOTS_DIR}" ]]; then PLOTS_DIR="${BENCH_DIR}/plots"; fi

  case "${RAW_DIR}" in /*) ;; *) RAW_DIR="${REPO_ROOT}/${RAW_DIR}" ;; esac
  case "${PLOTS_DIR}" in /*) ;; *) PLOTS_DIR="${REPO_ROOT}/${PLOTS_DIR}" ;; esac

  mkdir -p "${RAW_DIR}" "${PLOTS_DIR}"

  if [[ -n "${SUMMARY_OVERRIDE}" ]]; then
    if [[ -d "${SUMMARY_OVERRIDE}" || "${SUMMARY_OVERRIDE}" != *.csv ]]; then
      local dir="${SUMMARY_OVERRIDE}"
      case "${dir}" in /*) ;; *) dir="${REPO_ROOT}/${dir}" ;; esac
      mkdir -p "${dir}"
      SUMMARY_CSV="${dir}/summary.csv"
    else
      case "${SUMMARY_OVERRIDE}" in /*) SUMMARY_CSV="${SUMMARY_OVERRIDE}" ;; *) SUMMARY_CSV="${REPO_ROOT}/${SUMMARY_OVERRIDE}" ;; esac
      mkdir -p "$(dirname -- "${SUMMARY_CSV}")"
    fi
  else
    SUMMARY_CSV="${RAW_DIR}/summary.csv"
  fi
}

log() { echo "[$(date +%H:%M:%S)] $*"; }
run() { if [[ "${DRY_RUN}" = 1 ]]; then echo "+ $*"; else eval "$*"; fi }

usage() {
  cat <<EOF
QoS Comparison Benchmark - Compare MQTT QoS 0/1/2 latency across brokers

Usage: $(basename "$0") [OPTIONS]

Options:
  --pairs N              Number of pub/sub pairs (default: ${PAIRS})
  --total-rate N         Total messages per second (default: ${TOTAL_RATE})
  --payload N            Payload size in bytes (default: ${PAYLOAD})
  --duration N           Test duration in seconds (default: ${DURATION})
  --qos-levels "0 1 2"   QoS levels to test (default: "0 1 2")
  --mqtt-brokers "..."   Space-separated broker specs: name:host:port
  --host HOST            Override host for all brokers
  --sequential           Start/stop containers one by one (fairness)
  --ssh-target USER@HOST Remote execution target
  --remote-dir PATH      Remote mq-bench directory (default: ~/mq-bench)
  --start-services       Start local docker services before test
  --warmup N             Warmup duration in seconds (default: ${WARMUP_SECS})
  --ignore-start-secs N  Ignore first N seconds of data (default: ${IGNORE_START_SECS})
  --ignore-end-secs N    Ignore last N seconds of data (default: ${IGNORE_END_SECS})
  --interval-sec N       Seconds between tests (default: ${INTERVAL_SEC})
  --summary PATH         Append to existing summary CSV
  --dry-run              Print commands without executing
  -h, --help             Show this help

Example:
  # Basic QoS comparison across all MQTT brokers
  $(basename "$0") --sequential

  # Remote brokers with specific QoS levels
  $(basename "$0") --host 192.168.1.100 --qos-levels "0 1"

  # Higher rate test (QoS 2 may degrade)
  $(basename "$0") --total-rate 500 --pairs 20
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pairs)
      shift; PAIRS=${1:-10} ;;
    --total-rate)
      shift; TOTAL_RATE=${1:-100} ;;
    --payload)
      shift; PAYLOAD=${1:-1024} ;;
    --duration)
      shift; DURATION=${1:-30} ;;
    --snapshot)
      shift; SNAPSHOT=${1:-5} ;;
    --qos-levels)
      shift
      if [[ -n "${1:-}" ]]; then
        IFS=' ' read -r -a QOS_LEVELS <<<"${1}"
      fi
      ;;
    --mqtt-brokers)
      shift
      _raw="${1:-}"
      _raw=$(clean_quotes "${_raw}")
      MQTT_BROKERS="${_raw}"
      ;;
    --host)
      shift; HOST=${1:-} ;;
    --sequential)
      SEQUENTIAL=1 ;;
    --ssh-target)
      shift; SSH_TARGET=${1:-} ;;
    --remote-dir)
      shift; REMOTE_DIR=${1:-} ;;
    --start-services)
      START_SERVICES=1 ;;
    --warmup)
      shift; WARMUP_SECS=${1:-5} ;;
    --ignore-start-secs)
      shift; IGNORE_START_SECS=${1:-5} ;;
    --ignore-end-secs)
      shift; IGNORE_END_SECS=${1:-5} ;;
    --interval-sec)
      shift; INTERVAL_SEC=${1:-15} ;;
    --summary)
      shift; SUMMARY_OVERRIDE=${1:-}
      if is_quote_only_token "${SUMMARY_OVERRIDE}"; then SUMMARY_OVERRIDE=""; fi
      ;;
    --dry-run)
      DRY_RUN=1 ;;
    --run-id-prefix)
      shift; RUN_ID_PREFIX=${1:-qos} ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
  shift || true
done

# Initialize directories
init_dirs

# Infer HOST from SSH_TARGET if not set
if [[ -z "${HOST}" ]] && [[ -n "${SSH_TARGET}" ]]; then
  if [[ "${SSH_TARGET}" == *"@"* ]]; then
    HOST="${SSH_TARGET#*@}"
  else
    HOST="${SSH_TARGET}"
  fi
  log "Inferred HOST=${HOST} from SSH_TARGET"
fi

# Materialize MQTT brokers array
IFS=' ' read -r -a MQTT_BROKERS_ARR <<<"${MQTT_BROKERS}"

# Resolve broker names without host:port from defaults
declare -a _RESOLVED_BROKERS=()
for tok in "${MQTT_BROKERS_ARR[@]}"; do
  if [[ "${tok}" != *:* ]]; then
    found=""
    for def in ${DEFAULT_MQTT_BROKERS}; do
      IFS=: read -r dname dhost dport <<<"${def}"
      if [[ "${dname}" == "${tok}" ]]; then
        found="${def}"
        break
      fi
    done
    if [[ -n "${found}" ]]; then
      _RESOLVED_BROKERS+=("${found}")
    else
      log "WARN: Broker '${tok}' has no host:port and not found in defaults. Skipping."
    fi
  else
    _RESOLVED_BROKERS+=("${tok}")
  fi
done
MQTT_BROKERS_ARR=("${_RESOLVED_BROKERS[@]}")

# Apply HOST override if set
if [[ -n "${HOST}" ]]; then
  declare -a _REWRITTEN=()
  for tok in "${MQTT_BROKERS_ARR[@]}"; do
    IFS=: read -r bname _bhost bport <<<"${tok}"
    _REWRITTEN+=("${bname}:${HOST}:${bport}")
  done
  MQTT_BROKERS_ARR=("${_REWRITTEN[@]}")
fi

# CSV header with qos column
if [[ ! -s "${SUMMARY_CSV}" ]]; then
  echo "transport,broker,qos,payload,rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes,avg_cpu_perc,avg_mem_perc,avg_mem_used_bytes" > "${SUMMARY_CSV}"
fi

# Map broker to docker-compose service name
get_services() {
  case "$1" in
    mosquitto) echo "mosquitto" ;;
    emqx) echo "emqx" ;;
    hivemq) echo "hivemq" ;;
    rabbitmq) echo "rabbitmq" ;;
    artemis) echo "artemis" ;;
    *) echo "" ;;
  esac
}

# Run docker compose command (local or remote)
docker_compose_cmd() {
  local action="$1"
  local services="${2:-}"
  local cmd="docker compose ${action}"
  if [[ -n "$services" ]]; then cmd="$cmd $services"; fi

  if [[ -n "$SSH_TARGET" ]]; then
    local rcmd="cd ${REMOTE_DIR} && ${cmd}"
    log "[remote] ${SSH_TARGET}: ${rcmd}"
    if [[ "${DRY_RUN}" = 1 ]]; then
      echo "+ ssh ${SSH_TARGET} \"${rcmd}\""
    else
      ssh -o BatchMode=yes "${SSH_TARGET}" "${rcmd}"
    fi
  else
    log "[local] ${cmd}"
    run "${cmd}"
  fi
}

# Kill orphan mq-bench processes
cleanup_processes() {
  log "Cleaning up orphan mq-bench processes..."
  pkill -f "mq-bench.*mt-sub" 2>/dev/null || true
  pkill -f "mq-bench.*mt-pub" 2>/dev/null || true
  pkill -f "mq-bench.*sub" 2>/dev/null || true
  pkill -f "mq-bench.*pub" 2>/dev/null || true
  sleep 1
}

# Track started services
declare -A STARTED_SERVICES=()

# Manage service lifecycle
manage_service() {
  local action="$1"
  local services="$2"
  if [[ $SEQUENTIAL -eq 0 ]]; then return 0; fi
  if [[ -z "$services" ]]; then return 0; fi

  if [[ "$action" == "up" ]]; then
    log "Starting services: ${services}"
    cleanup_processes
    docker_compose_cmd down
    if [[ "${DRY_RUN}" != 1 ]]; then
      log "Waiting 10s for connections to settle..."
      sleep 10
    fi
    docker_compose_cmd "up -d" "$services"
    if [[ "${DRY_RUN}" != 1 ]]; then sleep 5; fi
    STARTED_SERVICES["$services"]=1
  elif [[ "$action" == "restart" ]]; then
    log "Full restarting services: ${services}"
    cleanup_processes
    docker_compose_cmd down
    if [[ "${DRY_RUN}" != 1 ]]; then
      log "Waiting 10s for connections to settle..."
      sleep 10
    fi
    docker_compose_cmd "up -d" "$services"
    if [[ "${DRY_RUN}" != 1 ]]; then sleep 5; fi
  elif [[ "$action" == "down" ]]; then
    log "Stopping services: ${services}"
    docker_compose_cmd down
  fi
}

# Wait for port to be ready
wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout="${3:-60}"
  log "Waiting for ${host}:${port}..."
  local start_ts=$(date +%s)
  while true; do
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; then
      log "Port ${host}:${port} is open."
      return 0
    fi
    local now_ts=$(date +%s)
    if (( now_ts - start_ts > timeout )); then
      log "Timeout waiting for ${host}:${port}"
      return 1
    fi
    sleep 1
  done
}

# Run warmup (results discarded)
run_warmup() {
  local broker_name="$1" broker_host="$2" broker_port="$3" qos="$4"

  if [[ ${WARMUP_SECS} -le 0 ]]; then return 0; fi

  log "Warmup: broker=${broker_name} qos=${qos} duration=${WARMUP_SECS}s"

  local per_rate=$(( TOTAL_RATE / PAIRS ))
  local env_common="TENANTS=${PAIRS} REGIONS=1 SERVICES=1 SHARDS=1 SUBSCRIBERS=${PAIRS} PUBLISHERS=${PAIRS} PAYLOAD=${WARMUP_PAYLOAD} DURATION=${WARMUP_SECS} SNAPSHOT=${WARMUP_SECS} RAMP_UP_SECS=1 RATE=${per_rate} TOPIC_PREFIX=bench/warmup"

  local host_env="MQTT_HOST=${broker_host} MQTT_PORT=${broker_port} MQTT_QOS=${qos}"
  if [[ "${broker_name}" == "artemis" ]]; then
    host_env="${host_env} MQTT_USERNAME=admin MQTT_PASSWORD=admin"
  fi

  local rid="warmup_$(timestamp)_${broker_name}_qos${qos}"
  run "ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\" >/dev/null 2>&1" || true

  rm -rf "${REPO_ROOT}/artifacts/${rid}" 2>/dev/null || true
  log "Warmup complete"
}

# Append results to summary CSV with steady-state filtering
append_summary_from_artifacts() {
  local broker="$1" qos="$2" payload="$3" total_rate="$4" run_id="$5" art_dir="$6"

  local sub_csv="${art_dir}/sub_agg.csv"
  local pub_csv="${art_dir}/mt_pub.csv"

  if [[ ! -f "${sub_csv}" ]]; then
    log "WARN: Missing subscriber CSV in ${art_dir}"
    return 0
  fi

  # Calculate steady-state metrics
  local steady_state_metrics
  steady_state_metrics=$(awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
    NR == 1 { next }
    {
        ts = $1 + 0
        if (min_ts == "" || ts < min_ts) min_ts = ts
        if (max_ts == "" || ts > max_ts) max_ts = ts

        row_ts[NR] = ts
        row_recv[NR] = $3 + 0
        row_sent[NR] = $2 + 0
        row_err[NR] = $4 + 0
        row_p50[NR] = $7 + 0
        row_p95[NR] = $8 + 0
        row_p99[NR] = $9 + 0
        total_rows = NR
    }
    END {
        window_start = min_ts + ignore_start
        window_end = max_ts - ignore_end

        for (i = 2; i <= total_rows; i++) {
            ts = row_ts[i]
            if (ts >= window_start && ts <= window_end && row_recv[i] > 0) {
                if (first_ts == "") {
                    first_ts = ts
                    first_recv = row_recv[i]
                    first_sent = row_sent[i]
                    first_err = row_err[i]
                }
                last_ts = ts
                last_recv = row_recv[i]
                last_sent = row_sent[i]
                last_err = row_err[i]

                sum_p50 += row_p50[i]
                sum_p95 += row_p95[i]
                sum_p99 += row_p99[i]
                count++
            }
        }

        if (count == 0) {
            print "0.00,0,0,0,0,0,0,0,0,0"
            exit
        }

        duration = last_ts - first_ts
        if (duration > 0) {
            tps = (last_recv - first_recv) / duration
        } else {
            tps = 0
        }

        avg_p50 = sum_p50 / count
        avg_p95 = sum_p95 / count
        avg_p99 = sum_p99 / count

        delta_recv = last_recv - first_recv
        delta_sent = last_sent - first_sent
        delta_err = last_err - first_err

        printf "%.2f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%d,%d,%d", tps, avg_p50, avg_p95, avg_p99, delta_sent, delta_recv, delta_err, count, first_ts, last_ts
    }
  ' "${sub_csv}")

  if [[ -z "${steady_state_metrics}" ]]; then
    log "WARN: No subscriber data for ${run_id}"
    return 0
  fi

  local tps avg_p50_ns avg_p95_ns avg_p99_ns sent recv _err steady_rows steady_start_ts steady_end_ts
  IFS=, read -r tps avg_p50_ns avg_p95_ns avg_p99_ns sent recv _err steady_rows steady_start_ts steady_end_ts <<<"${steady_state_metrics}"

  if [[ "${steady_rows}" -eq 0 ]]; then
    log "WARN: No steady-state rows found for ${run_id}"
    return 0
  fi

  log "Steady-state: ${steady_rows} rows, TPS=${tps}"

  # Publisher TPS
  local pub_tps=""
  if [[ -f "${pub_csv}" ]]; then
    pub_tps=$(awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
      NR == 1 { next }
      {
          ts = $1 + 0
          if (min_ts == "" || ts < min_ts) min_ts = ts
          if (max_ts == "" || ts > max_ts) max_ts = ts
          row_ts[NR] = ts
          row_sent[NR] = $2 + 0
          total_rows = NR
      }
      END {
          window_start = min_ts + ignore_start
          window_end = max_ts - ignore_end
          for (i = 2; i <= total_rows; i++) {
              ts = row_ts[i]
              if (ts >= window_start && ts <= window_end && row_sent[i] > 0) {
                  if (first_ts == "") { first_ts = ts; first_sent = row_sent[i] }
                  last_ts = ts; last_sent = row_sent[i]
              }
          }
          duration = last_ts - first_ts
          if (duration > 0) { printf "%.2f", (last_sent - first_sent) / duration }
          else { print "" }
      }
    ' "${pub_csv}")
  fi

  # Convert ns to ms
  local p50_ms p95_ms p99_ms
  p50_ms=$(awk -v n="${avg_p50_ns}" 'BEGIN{if(n==""||n==0){print ""}else{printf("%.3f", n/1e6)}}')
  p95_ms=$(awk -v n="${avg_p95_ns}" 'BEGIN{if(n==""||n==0){print ""}else{printf("%.3f", n/1e6)}}')
  p99_ms=$(awk -v n="${avg_p99_ns}" 'BEGIN{if(n==""||n==0){print ""}else{printf("%.3f", n/1e6)}}')

  # Docker stats (optional)
  local stats_csv="${art_dir}/docker_stats.csv"
  local max_cpu="" max_mem_perc="" max_mem_used="" avg_cpu="" avg_mem_perc="" avg_mem_used=""
  if [[ -f "${stats_csv}" ]]; then
    local agg
    agg=$(awk -F, -v start_ts="${steady_start_ts}" -v end_ts="${steady_end_ts}" '
      NR==1 { next }
      {
        ts = $1 + 0
        if (start_ts > 0 && end_ts > 0) {
          if (ts < start_ts || ts > end_ts) next
        }
        c=$17+0; used_b=$10+0; perc=$12+0
        if (c>mcpu) mcpu=c
        if (perc>mperc) mperc=perc
        if (used_b>mused) mused=used_b
        sum_cpu+=c; sum_mem_perc+=perc; sum_mem_used+=used_b; count++
      }
      END{
        if(count>0) { avg_cpu=sum_cpu/count; avg_mem_perc=sum_mem_perc/count; avg_mem_used=sum_mem_used/count }
        printf("%.6f,%.6f,%.0f,%.6f,%.6f,%.0f", mcpu+0,mperc+0,mused+0,avg_cpu+0,avg_mem_perc+0,avg_mem_used+0)
      }
    ' "${stats_csv}" || true)
    IFS=, read -r max_cpu max_mem_perc max_mem_used avg_cpu avg_mem_perc avg_mem_used <<<"${agg}"
  fi

  local transport="mqtt_${broker}"
  echo "${transport},${broker},${qos},${payload},${total_rate},${run_id},${tps},${p50_ms},${p95_ms},${p99_ms},${pub_tps},${sent},${recv},${_err},${art_dir},${max_cpu},${max_mem_perc},${max_mem_used},${avg_cpu},${avg_mem_perc},${avg_mem_used}" >> "${SUMMARY_CSV}"
}

# Run single QoS test
run_single_execution() {
  local broker_name="$1" broker_host="$2" broker_port="$3" qos="$4"

  local per_rate=$(( TOTAL_RATE / PAIRS ))
  local rid="${RUN_ID_PREFIX}_$(timestamp)_${broker_name}_qos${qos}_p${PAYLOAD}_r${TOTAL_RATE}"
  local art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_multi_topic_perkey"

  local env_common="TENANTS=${PAIRS} REGIONS=1 SERVICES=1 SHARDS=1 SUBSCRIBERS=${PAIRS} PUBLISHERS=${PAIRS} PAYLOAD=${PAYLOAD} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT} RAMP_UP_SECS=${RAMP_UP_SECS} RATE=${per_rate} TOPIC_PREFIX=bench/qos"

  local host_env="MQTT_HOST=${broker_host} MQTT_PORT=${broker_port} MQTT_QOS=${qos}"
  if [[ "${broker_name}" == "artemis" ]]; then
    host_env="${host_env} MQTT_USERNAME=admin MQTT_PASSWORD=admin"
  fi

  mkdir -p "${art_dir}"

  # Start docker stats collector
  local stats_pid=""
  local stats_csv="${art_dir}/docker_stats.csv"
  local stats_duration=$(( RAMP_UP_SECS + DURATION + 15 ))

  if [[ -n "${HOST}" ]]; then
    log "Starting remote stats collector on ${HOST} for ${stats_duration}s..."
    "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${HOST}" "${stats_csv}" "${stats_duration}" >/dev/null 2>&1 &
    stats_pid=$!
  else
    local mon_containers=()
    resolve_monitor_containers "${broker_name}" mon_containers
    if [[ ${#mon_containers[@]} -gt 0 ]]; then
      log "Starting local stats collector for: ${mon_containers[*]}"
      start_broker_stats_monitor stats_pid "${stats_csv}" "${mon_containers[@]}"
    fi
  fi

  log "Running: broker=${broker_name} qos=${qos} payload=${PAYLOAD} rate=${TOTAL_RATE}"
  run "ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""

  # Stop stats collector
  if [[ -n "${stats_pid}" ]] && (( stats_pid > 0 )); then
    kill "${stats_pid}" 2>/dev/null || true
    wait "${stats_pid}" 2>/dev/null || true
  fi

  append_summary_from_artifacts "${broker_name}" "${qos}" "${PAYLOAD}" "${TOTAL_RATE}" "${rid}" "${art_dir}"
}

# Ensure services are running (non-sequential mode)
ensure_services() {
  if [[ ${START_SERVICES} -eq 1 ]]; then
    if [[ -n "${HOST}" ]] && [[ "${HOST}" != "127.0.0.1" ]] && [[ "${HOST}" != "localhost" ]]; then
      log "HOST=${HOST} indicates remote brokers; skipping local service startup"
    else
      log "Starting local services via compose"
      run "docker compose up -d"
    fi
  fi
}

main() {
  log "QoS Comparison Benchmark → ${BENCH_DIR}"
  log "Brokers: ${MQTT_BROKERS_ARR[*]}"
  log "QoS levels: ${QOS_LEVELS[*]}"
  log "Config: pairs=${PAIRS} rate=${TOTAL_RATE} payload=${PAYLOAD} duration=${DURATION}s"

  # Increase ulimit if possible
  local ulim
  ulim=$(ulimit -n)
  if [[ "$ulim" != "unlimited" ]] && (( ulim < 65536 )); then
    log "Current ulimit -n is ${ulim}. Attempting to raise..."
    ulimit -n 65536 2>/dev/null || true
  fi

  if [[ $SEQUENTIAL -eq 0 ]]; then
    ensure_services
  fi

  for broker_spec in "${MQTT_BROKERS_ARR[@]}"; do
    IFS=: read -r broker_name broker_host broker_port <<<"${broker_spec}"
    log "=== Testing broker: ${broker_name} (${broker_host}:${broker_port}) ==="

    local svc=""
    if [[ $SEQUENTIAL -eq 1 ]]; then
      svc=$(get_services "${broker_name}")
    fi

    local first_qos=1
    for qos in "${QOS_LEVELS[@]}"; do
      # Service management for sequential mode
      if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
        if [[ $first_qos -eq 1 ]]; then
          manage_service up "$svc"
          wait_for_port "${broker_host}" "${broker_port}" 60
          first_qos=0
        else
          manage_service restart "$svc"
          wait_for_port "${broker_host}" "${broker_port}" 60
        fi
        run_warmup "${broker_name}" "${broker_host}" "${broker_port}" "${qos}"
      fi

      run_single_execution "${broker_name}" "${broker_host}" "${broker_port}" "${qos}"

      # Interval between tests
      if [[ "${INTERVAL_SEC}" -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
        log "Waiting ${INTERVAL_SEC}s before next test..."
        sleep "${INTERVAL_SEC}"
      fi
    done

    # Stop service after all QoS levels for this broker
    if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
      manage_service down "$svc"
    fi
  done

  log "Generating plots..."
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ python3 ${SCRIPT_DIR}/plot_qos_comparison.py --summary ${SUMMARY_CSV} --out-dir ${PLOTS_DIR}"
  else
    if [[ -f "${SCRIPT_DIR}/plot_qos_comparison.py" ]]; then
      python3 "${SCRIPT_DIR}/plot_qos_comparison.py" --summary "${SUMMARY_CSV}" --out-dir "${PLOTS_DIR}"
    else
      log "WARN: plot_qos_comparison.py not found; skipping plots"
    fi
  fi

  log "Done. Summary CSV: ${SUMMARY_CSV}"
  log "Plots directory: ${PLOTS_DIR}"
}

main "$@"
