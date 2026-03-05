#!/usr/bin/env bash
set -euo pipefail

# Orchestrate fanout subscriber sweep: run fanout tests from 1 to N subscribers.
# Supports multiple publishers (M pubs → N subs).
#
# The benchmark runs LOCALLY but connects to a REMOTE broker (via --host or inferred from --ssh-target).
# Use --ssh-target for docker compose operations (start/stop containers) on the remote machine.
#
# Usage:
#   scripts/orchestrate_fanout_sweep.sh [options]
#
# Options:
#   --max-subs N           Maximum number of subscribers (default: 10)
#   --pubs N               Number of publishers (default: 1)
#   --step N               Subscriber step increment: number, exp2, or exp10 (default: 1)
#   --rate N               Total message rate in msg/s (default: 10000)
#   --duration N           Test duration in seconds (default: 30)
#   --payload N            Payload size in bytes (default: 1024)
#   --engine ENGINE        Transport engine: zenoh|mqtt|redis|nats|amqp (default: zenoh)
#   --host HOST            Broker host (default: 127.0.0.1, auto-inferred from --ssh-target)
#   --port PORT            Broker port (auto-detected per engine if not specified)
#   --interval N           Sleep seconds between runs (default: 5)
#   --ignore-start-secs N  Seconds to ignore from start of data (default: 5)
#   --ignore-end-secs N    Seconds to ignore from end of data (default: 5)
#   --ssh-target USER@HOST SSH target for docker compose operations (broker machine)
#   --remote-dir PATH      Remote working directory for docker compose (default: ~/mq-bench)
#   --sequential           Start/stop broker containers per run
#   --append-latest        Append to most recent fanout_sweep_* results directory
#   --dry-run              Print commands without executing
#   -h, --help             Show this help
#
# Examples:
#   # Local broker sweep 1-10 subscribers
#   scripts/orchestrate_fanout_sweep.sh --max-subs 10 --rate 10000
#
#   # Remote broker: benchmark runs locally, connects to remote zenoh router
#   scripts/orchestrate_fanout_sweep.sh --host 192.168.0.245 --engine zenoh --max-subs 20
#
#   # Remote broker with sequential mode: SSH controls docker on remote, benchmark runs locally
#   scripts/orchestrate_fanout_sweep.sh --ssh-target ubuntu@192.168.0.245 --remote-dir /home/ubuntu \
#     --pubs 100 --engine zenoh --duration 120 --sequential
#
#   # Custom subscriber list
#   SUBS_LIST="1 5 10 20 50" scripts/orchestrate_fanout_sweep.sh --pubs 100 --rate 1000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
MAX_SUBS=10
PUBS=1
STEP=1
RATE=10000
DURATION=30
PAYLOAD=1024
ENGINE=zenoh
HOST=""
PORT=""
INTERVAL=5
DRY_RUN=0
SUBS_LIST="${SUBS_LIST:-}"
IGNORE_START_SECS=5
IGNORE_END_SECS=5
SSH_TARGET=""
REMOTE_DIR="~/mq-bench"
SEQUENTIAL=0
APPEND_LATEST=0

timestamp() { date +%Y%m%d_%H%M%S; }
log() { echo "[$(date +%H:%M:%S)] $*"; }

usage() {
  sed -n '1,50p' "$0" | grep "^#" | sed 's/^# \?//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-subs) shift; MAX_SUBS=${1:-10} ;;
    --pubs) shift; PUBS=${1:-1} ;;
    --step) shift; STEP=${1:-1} ;;
    --rate) shift; RATE=${1:-10000} ;;
    --duration) shift; DURATION=${1:-30} ;;
    --payload) shift; PAYLOAD=${1:-1024} ;;
    --engine) shift; ENGINE=${1:-zenoh} ;;
    --host) shift; HOST=${1:-} ;;
    --port) shift; PORT=${1:-} ;;
    --interval) shift; INTERVAL=${1:-5} ;;
    --ignore-start-secs) shift; IGNORE_START_SECS=${1:-5} ;;
    --ignore-end-secs) shift; IGNORE_END_SECS=${1:-5} ;;
    --ssh-target) shift; SSH_TARGET=${1:-} ;;
    --remote-dir) shift; REMOTE_DIR=${1:-} ;;
    --sequential) SEQUENTIAL=1 ;;
    --append-latest) APPEND_LATEST=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
  shift || true
done

# Infer HOST from SSH_TARGET if not set
if [[ -z "${HOST}" ]] && [[ -n "${SSH_TARGET}" ]]; then
  if [[ "${SSH_TARGET}" == *"@"* ]]; then
    HOST="${SSH_TARGET#*@}"
  else
    HOST="${SSH_TARGET}"
  fi
  log "Inferred HOST=${HOST} from SSH_TARGET"
fi

# Default HOST if still empty
if [[ -z "${HOST}" ]]; then
  HOST="127.0.0.1"
fi

# Auto-detect port based on engine if not specified
if [[ -z "${PORT}" ]]; then
  case "${ENGINE}" in
    zenoh) PORT=7447 ;;
    mqtt) PORT=1883 ;;
    redis) PORT=6379 ;;
    nats) PORT=4222 ;;
    amqp|rabbitmq) PORT=5672 ;;
    *) PORT=7447 ;;
  esac
fi

# Build subscriber list
declare -a SUBS_ARRAY=()
if [[ -n "${SUBS_LIST}" ]]; then
  IFS=' ,' read -r -a SUBS_ARRAY <<<"${SUBS_LIST}"
elif [[ "${STEP}" == "exp2" ]]; then
  n=1
  while (( n <= MAX_SUBS )); do
    SUBS_ARRAY+=("$n")
    n=$((n * 2))
  done
elif [[ "${STEP}" == "exp10" ]]; then
  n=1
  while (( n <= MAX_SUBS )); do
    SUBS_ARRAY+=("$n")
    n=$((n * 10))
  done
else
  for (( n=1; n<=MAX_SUBS; n+=STEP )); do
    SUBS_ARRAY+=("$n")
  done
fi

# Setup output directory
TS="$(timestamp)"
BENCH_DIR=""
RAW_DIR=""
SUMMARY_CSV=""

init_dirs() {
  if [[ ${APPEND_LATEST} -eq 1 ]] && [[ -z "${BENCH_DIR}" ]]; then
    local latest_dir
    latest_dir=$(ls -1d "${REPO_ROOT}/results/fanout_sweep_"* 2>/dev/null | sort -r | head -1 || true)
    if [[ -n "${latest_dir}" ]] && [[ -d "${latest_dir}" ]]; then
      BENCH_DIR="${latest_dir}"
      RAW_DIR="${BENCH_DIR}/raw_data"
      TS="${latest_dir##*fanout_sweep_}"
      log "Appending to existing run: ${BENCH_DIR}"
    else
      log "WARN: --append-latest specified but no existing fanout_sweep_* directory found. Creating new."
    fi
  fi
  
  if [[ -z "${BENCH_DIR}" ]]; then
    BENCH_DIR="${REPO_ROOT}/results/fanout_sweep_${TS}"
  fi
  if [[ -z "${RAW_DIR}" ]]; then
    RAW_DIR="${BENCH_DIR}/raw_data"
  fi
  SUMMARY_CSV="${RAW_DIR}/summary.csv"
  mkdir -p "${RAW_DIR}"
}

init_dirs

log "Fanout subscriber sweep: ${ENGINE} @ ${HOST}:${PORT}"
log "Publishers: ${PUBS} | Subscribers: ${SUBS_ARRAY[*]}"
log "Rate: ${RATE} msg/s | Duration: ${DURATION}s | Payload: ${PAYLOAD}B"
log "Ignore: start=${IGNORE_START_SECS}s end=${IGNORE_END_SECS}s"
if [[ -n "${SSH_TARGET}" ]]; then
  log "Remote execution: ${SSH_TARGET}:${REMOTE_DIR}"
fi
log "Output: ${BENCH_DIR}"

# Write summary CSV header if file doesn't exist or is empty
if [[ ! -s "${SUMMARY_CSV}" ]]; then
  echo "engine,host,port,pubs,subs,rate,payload,duration,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,loss_pct,artifacts_dir" > "${SUMMARY_CSV}"
fi

# Build engine-specific environment
build_env() {
  local subs="$1"
  local env_str="ENGINE=${ENGINE} PUBS=${PUBS} SUBS=${subs} RATE=${RATE} DURATION=${DURATION} PAYLOAD=${PAYLOAD}"
  
  case "${ENGINE}" in
    zenoh)
      env_str+=" ENDPOINT_PUB=tcp/${HOST}:${PORT} ENDPOINT_SUB=tcp/${HOST}:${PORT}"
      ;;
    mqtt)
      env_str+=" MQTT_HOST=${HOST} MQTT_PORT=${PORT}"
      ;;
    redis)
      env_str+=" REDIS_URL=redis://${HOST}:${PORT}"
      ;;
    nats)
      env_str+=" NATS_HOST=${HOST} NATS_PORT=${PORT}"
      ;;
    amqp|rabbitmq)
      env_str+=" ENGINE=rabbitmq RABBITMQ_HOST=${HOST} RABBITMQ_PORT=${PORT}"
      ;;
  esac
  
  echo "${env_str}"
}

# Get docker-compose service name for engine
get_service_name() {
  case "$1" in
    zenoh) echo "router1" ;;
    mqtt) echo "mosquitto" ;;
    redis) echo "redis" ;;
    nats) echo "nats" ;;
    amqp|rabbitmq) echo "rabbitmq" ;;
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
    # Remote execution for docker compose only
    local rcmd="cd ${REMOTE_DIR} && ${cmd}"
    log "[remote] ${SSH_TARGET}: ${rcmd}"
    if [[ ${DRY_RUN} -eq 1 ]]; then
      echo "+ ssh ${SSH_TARGET} \"${rcmd}\""
    else
      ssh -o BatchMode=yes "${SSH_TARGET}" "${rcmd}"
    fi
  else
    # Local execution
    log "[local] ${cmd}"
    if [[ ${DRY_RUN} -eq 1 ]]; then
      echo "+ ${cmd}"
    else
      eval "${cmd}"
    fi
  fi
}

# Wait for a TCP port to become available
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

# Manage service lifecycle if sequential mode is enabled
manage_service() {
  local action="$1" # up or down
  local svc="$2"
  if [[ ${SEQUENTIAL} -eq 0 ]]; then return 0; fi
  if [[ -z "$svc" ]]; then return 0; fi
  
  if [[ "$action" == "up" ]]; then
    log "Starting service: ${svc}"
    docker_compose_cmd "up -d" "$svc"
    sleep 3  # Give broker time to initialize
  elif [[ "$action" == "down" ]]; then
    log "Stopping service: ${svc}"
    docker_compose_cmd "stop" "$svc"
  fi
}

# Extract metrics from artifact CSVs with steady-state filtering
extract_metrics() {
  local art_dir="$1"
  local sub_csv="${art_dir}/sub_agg.csv"
  local pub_csv="${art_dir}/pub_agg.csv"
  
  # Fallback to pub.csv if pub_agg.csv doesn't exist
  if [[ ! -f "${pub_csv}" ]]; then
    pub_csv="${art_dir}/pub.csv"
  fi
  
  if [[ ! -f "${sub_csv}" ]]; then
    echo ",,,,,,,,"
    return
  fi
  
  # Calculate steady-state metrics with ignore_start/end filtering
  local metrics
  metrics=$(awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
    NR == 1 { next }
    {
      ts = $1 + 0
      if (min_ts == "" || ts < min_ts) min_ts = ts
      if (max_ts == "" || ts > max_ts) max_ts = ts
      row_ts[NR] = ts
      row_recv[NR] = $3 + 0
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
            first_err = row_err[i]
          }
          last_ts = ts
          last_recv = row_recv[i]
          last_err = row_err[i]
          sum_p50 += row_p50[i]
          sum_p95 += row_p95[i]
          sum_p99 += row_p99[i]
          count++
        }
      }
      
      if (count == 0) {
        print "0,0,0,0,0,0"
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
      delta_err = last_err - first_err
      
      printf "%.2f,%.3f,%.3f,%.3f,%.0f,%.0f", tps, avg_p50/1e6, avg_p95/1e6, avg_p99/1e6, delta_recv, delta_err
    }
  ' "${sub_csv}")
  
  local pub_tps=""
  local sent=""
  if [[ -f "${pub_csv}" ]]; then
    local pub_metrics
    pub_metrics=$(awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
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
        delta_sent = last_sent - first_sent
        if (duration > 0) { printf "%.2f,%.0f", delta_sent/duration, delta_sent }
        else { print "0,0" }
      }
    ' "${pub_csv}")
    IFS=, read -r pub_tps sent <<<"${pub_metrics}"
  fi
  
  echo "${metrics},${pub_tps},${sent}"
}

# Calculate loss percentage
calc_loss_pct() {
  local sent="$1" recv="$2"
  awk -v s="${sent}" -v r="${recv}" 'BEGIN{if(s>0){printf("%.2f", (s-r)/s*100)}else{print "0.00"}}'
}

# Get service name for sequential mode
SERVICE_NAME=""
if [[ ${SEQUENTIAL} -eq 1 ]]; then
  SERVICE_NAME=$(get_service_name "${ENGINE}")
fi

# Main sweep loop
for subs in "${SUBS_ARRAY[@]}"; do
  run_id="fanout_${TS}_${ENGINE}_p${PUBS}_s${subs}_r${RATE}"
  art_dir="${REPO_ROOT}/artifacts/${run_id}/fanout_singlesite"
  
  log "Run: pubs=${PUBS} subs=${subs} rate=${RATE} -> ${run_id}"
  
  env_cmd=$(build_env "${subs}")
  
  # Sequential mode: start service before run
  if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${SERVICE_NAME}" ]]; then
    manage_service up "${SERVICE_NAME}"
    wait_for_port "${HOST}" "${PORT}"
  fi
  
  if [[ ${DRY_RUN} -eq 1 ]]; then
    echo "+ ${env_cmd} bash ${SCRIPT_DIR}/run_fanout.sh ${run_id}"
  else
    # Run benchmark LOCALLY, connecting to remote broker via HOST
    eval "${env_cmd} bash ${SCRIPT_DIR}/run_fanout.sh ${run_id}" || {
      log "WARN: Run failed for pubs=${PUBS} subs=${subs}"
    }
    
    # Extract and append metrics
    metrics=$(extract_metrics "${art_dir}")
    IFS=, read -r sub_tps p50 p95 p99 recv err pub_tps sent <<<"${metrics}"
    
    # Calculate loss
    loss_pct=$(calc_loss_pct "${sent}" "${recv}")
    
    echo "${ENGINE},${HOST},${PORT},${PUBS},${subs},${RATE},${PAYLOAD},${DURATION},${run_id},${sub_tps},${p50},${p95},${p99},${pub_tps},${sent},${recv},${err},${loss_pct},${art_dir}" >> "${SUMMARY_CSV}"
    
    log "  -> sub_tps=${sub_tps} p99=${p99}ms recv=${recv} loss=${loss_pct}%"
  fi
  
  # Sequential mode: stop service after run
  if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${SERVICE_NAME}" ]]; then
    manage_service down "${SERVICE_NAME}"
  fi
  
  # Cooldown between runs
  if [[ ${INTERVAL} -gt 0 ]] && [[ ${DRY_RUN} -eq 0 ]]; then
    log "Cooldown: ${INTERVAL}s..."
    sleep "${INTERVAL}"
  fi
done

log "Done. Summary: ${SUMMARY_CSV}"
log "To plot: python3 scripts/plot_results.py --summary ${SUMMARY_CSV} --out-dir ${BENCH_DIR}/plots"
