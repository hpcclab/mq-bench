#!/usr/bin/env bash
set -euo pipefail

# Orchestrate fanout experiment across transports.
#
# Topology: 1 publisher → N subscribers (all receive every message on one topic)
# Workload: fixed payload and total rate; sweep N in a configured list.
# Runner: uses scripts/run_fanout.sh for each (pubs=1, subs=N) combination
# Output: results/fanout_<ts>/{raw_data,plots}/ with summary.csv and figures
#
# Usage examples:
#   # Default: payload=1024B, subs list "1 2 4 8 16 32 64", rate=10000 msg/s
#   scripts/orchestrate_fanout.sh
#
#   # Custom subs sweep and rate
#   scripts/orchestrate_fanout.sh --subs-list "1 4 16 64 256" --rate 5000
#
#   # Limit to specific transports
#   scripts/orchestrate_fanout.sh --transports "zenoh nats redis"
#
#   # Remote host for all brokers (rewrite default ports)
#   scripts/orchestrate_fanout.sh --host 192.168.0.254
#
#   # MQTT multi-broker support:
#   scripts/orchestrate_fanout.sh --transports "mqtt" --mqtt-brokers "mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884"
#
#   # Append new runs into an existing summary:
#   scripts/orchestrate_fanout.sh --summary results/fanout_YYYYMMDD_HHMMSS/raw_data/summary.csv --transports "redis" --subs-list "1 4"
#
#   # Start services automatically, then stop each one between runs:
#   scripts/orchestrate_fanout.sh --start-services --sequential
#
#   # Dry-run: print commands without executing
#   scripts/orchestrate_fanout.sh --dry-run --transports "zenoh" --subs-list "1 4"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Source lib.sh for docker stats monitoring functions
source "${SCRIPT_DIR}/lib.sh"

# Defaults
SUBS_LIST=(1 2 4 8 16 32 64)
PUBS=1                        # fixed publisher count
PAYLOAD_TOKEN="1024"          # accepts KB/MB suffix; converted to bytes
RATE=10000                    # total msg/s (run_fanout.sh divides by PUBS internally)
DURATION=30
SNAPSHOT=1
RAMP_UP_SECS=5
WARMUP_SECS=0                 # warmup duration (0 to disable)
WARMUP_PAYLOAD=1024           # warmup payload size in bytes
IGNORE_START_SECS=5           # seconds to ignore from beginning of data
IGNORE_END_SECS=5             # seconds to ignore from end of data
RUN_ID_PREFIX="fanout"
TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
DEFAULT_TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
START_SERVICES=0
DRY_RUN=${DRY_RUN:-0}
HOST="${HOST:-}"
SUMMARY_OVERRIDE="${SUMMARY_OVERRIDE:-}"
INTERVAL_SEC=65  # Allow TIME_WAIT sockets to clear (default kernel timeout is 60s)

# Sequential / Remote execution
SEQUENTIAL=0
SSH_TARGET=""
REMOTE_DIR="~/mq-bench"
APPEND_LATEST=0              # append to most recent results directory

# MQTT brokers (name:host:port). Default to local compose ports.
MQTT_BROKERS="mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885 rabbitmq:127.0.0.1:1886 artemis:127.0.0.1:1887"
DEFAULT_MQTT_BROKERS="${MQTT_BROKERS}"
declare -a MQTT_BROKERS_ARR=()

# AMQP brokers (name:host:port). Default to local compose ports.
AMQP_BROKERS="rabbitmq:127.0.0.1:5672"
DEFAULT_AMQP_BROKERS="${AMQP_BROKERS}"
declare -a AMQP_BROKERS_ARR=()

# Helper to clean quotes from a string (strip leading/trailing ASCII quotes)
clean_quotes() {
  local v="$1"
  # Strip leading quote
  [[ "${v:0:1}" == '"' || "${v:0:1}" == "'" ]] && v="${v:1}"
  # Strip trailing quote
  local last="${v: -1}"
  [[ "$last" == '"' || "$last" == "'" ]] && v="${v:0:${#v}-1}"
  echo "$v"
}

# Convert tokens with KB/MB suffix to bytes
to_bytes() {
  local tok="$1"
  if [[ "$tok" =~ ^[0-9]+$ ]]; then echo "$tok"; return 0; fi
  if [[ "$tok" =~ ^([0-9]+)[Kk][Bb]?$ ]]; then echo $(( ${BASH_REMATCH[1]} * 1024 )); return 0; fi
  if [[ "$tok" =~ ^([0-9]+)[Mm][Bb]?$ ]]; then echo $(( ${BASH_REMATCH[1]} * 1024 * 1024 )); return 0; fi
  if [[ "$tok" =~ ^([0-9]+)[bB]$ ]]; then echo "${BASH_REMATCH[1]}"; return 0; fi
  echo "$tok"
}

is_quote_only_token() {
  local v="$1"
  case "$v" in
    "\"\""|"''") return 0;;
    *) return 1;;
  esac
}

timestamp() { date +%Y%m%d_%H%M%S; }
TS=""
BENCH_DIR=""
RAW_DIR=""
PLOTS_DIR=""
SUMMARY_CSV=""

init_dirs() {
  # Handle --append-latest: find and use most recent results directory
  if [[ ${APPEND_LATEST} -eq 1 ]] && [[ -z "${SUMMARY_OVERRIDE}" ]] && [[ -z "${BENCH_DIR}" ]]; then
    local latest_dir
    latest_dir=$(ls -1d "${REPO_ROOT}/results/fanout_"* 2>/dev/null | sort -r | head -1 || true)
    if [[ -n "${latest_dir}" ]] && [[ -d "${latest_dir}" ]]; then
      BENCH_DIR="${latest_dir}"
      RAW_DIR="${BENCH_DIR}/raw_data"
      PLOTS_DIR="${BENCH_DIR}/plots"
      TS="${latest_dir##*fanout_}"
      log "Appending to existing run: ${BENCH_DIR}"
    else
      log "WARN: --append-latest specified but no existing fanout_* directory found. Creating new."
    fi
  fi

  if [[ -z "${TS}" ]]; then TS="$(timestamp)"; fi
  if [[ -z "${BENCH_DIR}" ]]; then BENCH_DIR="${REPO_ROOT}/results/fanout_${TS}"; fi
  if [[ -z "${RAW_DIR}" ]]; then RAW_DIR="${BENCH_DIR}/raw_data"; fi
  if [[ -z "${PLOTS_DIR}" ]]; then PLOTS_DIR="${BENCH_DIR}/plots"; fi
  case "${RAW_DIR}" in /*) ;; *) RAW_DIR="${REPO_ROOT}/${RAW_DIR}" ;; esac
  case "${PLOTS_DIR}" in /*) ;; *) PLOTS_DIR="${REPO_ROOT}/${PLOTS_DIR}" ;; esac
  mkdir -p "${RAW_DIR}" "${PLOTS_DIR}"
  if [[ -n "${SUMMARY_OVERRIDE}" ]]; then
    if [[ -d "${SUMMARY_OVERRIDE}" || "${SUMMARY_OVERRIDE}" != *.csv ]]; then
      local dir="${SUMMARY_OVERRIDE}"; case "${dir}" in /*) ;; *) dir="${REPO_ROOT}/${dir}" ;; esac
      mkdir -p "${dir}"; SUMMARY_CSV="${dir}/summary.csv"
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
  sed -n '1,120p' "$0" | sed -n 's/^# //p'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subs-list)
      shift; IFS=' ' read -r -a SUBS_LIST <<<"${1:-}" ;;
    --pubs)
      shift; PUBS=${1:-1} ;;
    --payload)
      shift; PAYLOAD_TOKEN=${1:-1024} ;;
    --rate|--total-rate)
      shift; RATE=${1:-10000} ;;
    --duration)
      shift; DURATION=${1:-30} ;;
    --snapshot)
      shift; SNAPSHOT=${1:-1} ;;
    --transports)
      shift; if [[ -n "${1:-}" ]]; then IFS=' ' read -r -a TRANSPORTS <<<"${1}"; fi ;;
    --run-id-prefix)
      shift; RUN_ID_PREFIX=${1:-fanout} ;;
    --host)
      shift; HOST=${1:-} ;;
    --start-services)
      START_SERVICES=1 ;;
    --dry-run)
      DRY_RUN=1 ;;
    --summary)
      shift; SUMMARY_OVERRIDE=${1:-}; if is_quote_only_token "${SUMMARY_OVERRIDE}"; then SUMMARY_OVERRIDE=""; fi ;;
    --out-dir)
      shift; PLOTS_DIR=${1:-}; if is_quote_only_token "${PLOTS_DIR}"; then PLOTS_DIR=""; fi ;;
    --raw-dir)
      shift; RAW_DIR=${1:-}; if is_quote_only_token "${RAW_DIR}"; then RAW_DIR=""; fi ;;
    --bench-dir)
      shift; BENCH_DIR=${1:-}; if is_quote_only_token "${BENCH_DIR}"; then BENCH_DIR=""; fi ;;
    --mqtt-brokers)
      shift
      _raw_brokers="${1:-}"
      _raw_brokers=$(clean_quotes "${_raw_brokers}")
      MQTT_BROKERS="${_raw_brokers}"
      ;;
    --amqp-brokers)
      shift
      _raw_brokers="${1:-}"
      _raw_brokers=$(clean_quotes "${_raw_brokers}")
      AMQP_BROKERS="${_raw_brokers}"
      ;;
    --interval-sec)
      shift; INTERVAL_SEC=${1:-0} ;;
    --warmup)
      shift; WARMUP_SECS=${1:-10} ;;
    --warmup-payload)
      shift; WARMUP_PAYLOAD=${1:-1024} ;;
    --ignore-start-secs)
      shift; IGNORE_START_SECS=${1:-5} ;;
    --ignore-end-secs)
      shift; IGNORE_END_SECS=${1:-5} ;;
    --sequential)
      SEQUENTIAL=1 ;;
    --ssh-target)
      shift; SSH_TARGET=${1:-} ;;
    --remote-dir)
      shift; REMOTE_DIR=${1:-} ;;
    --append-latest)
      APPEND_LATEST=1 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
  shift || true
done

init_dirs

# Optionally start local services unless HOST indicates remote
ensure_services() {
  if [[ ${START_SERVICES} -eq 1 ]]; then
    if [[ -n "${HOST}" ]] && [[ "${HOST}" != "127.0.0.1" ]] && [[ "${HOST}" != "localhost" ]]; then
      log "HOST=${HOST} indicates remote brokers; skipping local service startup"
    else
      log "Starting local services via compose_up.sh"
      run "bash \"${SCRIPT_DIR}/compose_up.sh\""
    fi
  fi
}

# Resolve payload bytes
PAYLOAD_BYTES="$(to_bytes "${PAYLOAD_TOKEN}")"
if [[ ! "${PAYLOAD_BYTES}" =~ ^[0-9]+$ ]]; then
  echo "[error] Invalid --payload: ${PAYLOAD_TOKEN}" >&2; exit 2
fi

# Guardrails
if [[ ${#TRANSPORTS[@]} -eq 0 ]]; then TRANSPORTS=("${DEFAULT_TRANSPORTS[@]}"); fi

# Infer HOST from SSH_TARGET if not set
if [[ -z "${HOST}" ]] && [[ -n "${SSH_TARGET}" ]]; then
  if [[ "${SSH_TARGET}" == *"@"* ]]; then
    HOST="${SSH_TARGET#*@}"
  else
    HOST="${SSH_TARGET}"
  fi
  log "Inferred HOST=${HOST} from SSH_TARGET"
fi

# Materialize MQTT brokers array and apply HOST rewrite if needed
IFS=' ' read -r -a MQTT_BROKERS_ARR <<<"${MQTT_BROKERS}"

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

if [[ -n "${HOST}" ]]; then
  declare -a _REWRITTEN=()
  for tok in "${MQTT_BROKERS_ARR[@]}"; do IFS=: read -r bname _bhost bport <<<"${tok}"; _REWRITTEN+=("${bname}:${HOST}:${bport}"); done
  MQTT_BROKERS_ARR=("${_REWRITTEN[@]}")
fi

# Materialize AMQP brokers array and apply HOST rewrite if needed
IFS=' ' read -r -a AMQP_BROKERS_ARR <<<"${AMQP_BROKERS}"

declare -a _RESOLVED_AMQP_BROKERS=()
for tok in "${AMQP_BROKERS_ARR[@]}"; do
  if [[ "${tok}" != *:* ]]; then
    found=""
    for def in ${DEFAULT_AMQP_BROKERS}; do
      IFS=: read -r dname dhost dport <<<"${def}"
      if [[ "${dname}" == "${tok}" ]]; then
        found="${def}"
        break
      fi
    done
    if [[ -n "${found}" ]]; then
      _RESOLVED_AMQP_BROKERS+=("${found}")
    else
      log "WARN: AMQP Broker '${tok}' has no host:port and not found in defaults. Skipping."
    fi
  else
    _RESOLVED_AMQP_BROKERS+=("${tok}")
  fi
done
AMQP_BROKERS_ARR=("${_RESOLVED_AMQP_BROKERS[@]}")

if [[ -n "${HOST}" ]]; then
  declare -a _REWRITTEN=()
  for tok in "${AMQP_BROKERS_ARR[@]}"; do IFS=: read -r bname _bhost bport <<<"${tok}"; _REWRITTEN+=("${bname}:${HOST}:${bport}"); done
  AMQP_BROKERS_ARR=("${_REWRITTEN[@]}")
fi

# Header
if [[ ! -s "${SUMMARY_CSV}" ]]; then
  echo "transport,payload,subs,rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,loss_pct,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes,avg_cpu_perc,avg_mem_perc,avg_mem_used_bytes" > "${SUMMARY_CSV}"
fi

append_summary_from_artifacts() {
  local transport="$1" payload="$2" subs="$3" rate="$4" run_id="$5" art_dir="$6"
  local sub_csv pub_csv
  # run_fanout.sh writes sub_agg.csv; fall back to sub.csv
  sub_csv="${art_dir}/sub_agg.csv"
  pub_csv="${art_dir}/pub_agg.csv"
  if [[ ! -f "${sub_csv}" ]]; then
    if [[ -f "${art_dir}/sub.csv" ]]; then sub_csv="${art_dir}/sub.csv"; else log "WARN: Missing subscriber CSV in ${art_dir}"; return 0; fi
  fi
  if [[ ! -f "${pub_csv}" ]]; then
    if [[ -f "${art_dir}/pub.csv" ]]; then pub_csv="${art_dir}/pub.csv"; fi
  fi

  # Calculate steady-state metrics by ignoring first IGNORE_START_SECS and last IGNORE_END_SECS
  # Columns: 1=timestamp, 2=sent_count, 3=received_count, 4=error_count, 5=total_throughput,
  #          6=interval_throughput, 7=p50, 8=p95, 9=p99, 10=min, 11=max, 12=mean, 13=connections, 14=active_connections
  local steady_state_metrics
  steady_state_metrics=$(awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
    NR == 1 { next }  # skip header
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
        row_mean[NR] = $12 + 0
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
                sum_mean += row_mean[i]
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
    log "WARN: No steady-state rows found for ${run_id} (ignore_start=${IGNORE_START_SECS}s, ignore_end=${IGNORE_END_SECS}s)"
    return 0
  fi

  log "Steady-state: ${steady_rows} rows (ignore_start=${IGNORE_START_SECS}s, ignore_end=${IGNORE_END_SECS}s), TPS=${tps}, time_range=${steady_start_ts}-${steady_end_ts}"

  # Compute loss_pct from steady-state deltas
  local loss_pct
  loss_pct=$(awk -v s="${sent}" -v r="${recv}" 'BEGIN{
    if (s > 0) { printf "%.2f", (s - r) / s * 100 }
    else { print "0.00" }
  }')

  # Calculate publisher TPS using same time-based filtering
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

  # Convert latencies from ns to ms
  local p50_ms p95_ms p99_ms
  p50_ms=$(awk -v n="${avg_p50_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')
  p95_ms=$(awk -v n="${avg_p95_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')
  p99_ms=$(awk -v n="${avg_p99_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')

  # Docker stats aggregation (optional) - filtered to steady-state time range
  local stats_csv="${art_dir}/docker_stats.csv" max_cpu max_mem_perc max_mem_used avg_cpu avg_mem_perc avg_mem_used
  max_cpu=""; max_mem_perc=""; max_mem_used=""
  avg_cpu=""; avg_mem_perc=""; avg_mem_used=""
  if [[ -f "${stats_csv}" ]]; then
    local agg
    agg=$(awk -F, -v start_ts="${steady_start_ts}" -v end_ts="${steady_end_ts}" '
      NR==1 {
        is_remote=(NF==5 && $3=="cpu_perc");
        has_memprec=(NF>=12); has_cpuprec=(NF>=17);
        next
      }
      {
        ts = $1 + 0
        if (start_ts > 0 && end_ts > 0) {
          if (ts < start_ts || ts > end_ts) next
        }

        if (is_remote) {
           c=$3; gsub(/%/,"",c); c+=0
           perc=$4; gsub(/%/,"",perc); perc+=0
           used_str=$5;
           split(used_str, parts, " ");
           used_part = parts[1];
           val=used_part+0;
           unit=used_part; gsub(/[0-9.]/,"",unit);
           if (index(unit,"Gi")>0) val*=1024*1024*1024;
           else if (index(unit,"Mi")>0) val*=1024*1024;
           else if (index(unit,"Ki")>0) val*=1024;
           used_b=val
        } else {
           if (has_cpuprec) { c=$17+0 } else { c=$4; gsub(/%/,"",c); c+=0 }
           if (has_memprec) {
             used_b=$10+0; tot_b=$11+0; if (used_b>mused) mused=used_b; perc=$12+0; if (perc==0 && tot_b>0) perc=(used_b/tot_b)*100.0;
           }
        }

        if (c>mcpu) mcpu=c
        if (perc>mperc) mperc=perc
        if (used_b>mused) mused=used_b

        sum_cpu+=c
        sum_mem_perc+=perc
        sum_mem_used+=used_b
        count++
      }
      END{
        if(mcpu=="")mcpu=0; if(mperc=="")mperc=0; if(mused=="")mused=0
        if(count>0) {
          avg_cpu=sum_cpu/count; avg_mem_perc=sum_mem_perc/count; avg_mem_used=sum_mem_used/count
        } else {
          avg_cpu=0; avg_mem_perc=0; avg_mem_used=0
        }
        printf("%.6f,%.6f,%.0f,%.6f,%.6f,%.0f,%d", mcpu,mperc,mused,avg_cpu,avg_mem_perc,avg_mem_used,count)
      }
    ' "${stats_csv}" || true)
    local stats_rows
    IFS=, read -r max_cpu max_mem_perc max_mem_used avg_cpu avg_mem_perc avg_mem_used stats_rows <<<"${agg}"
    log "Docker stats: ${stats_rows} rows in steady-state time range"
  fi

  echo "${transport},${payload},${subs},${rate},${run_id},${tps},${p50_ms},${p95_ms},${p99_ms},${pub_tps},${sent},${recv},${_err},${loss_pct},${art_dir},${max_cpu},${max_mem_perc},${max_mem_used},${avg_cpu},${avg_mem_perc},${avg_mem_used}" >> "${SUMMARY_CSV}"
}

# Map transport/broker to docker-compose service names
get_services() {
  case "$1" in
    zenoh) echo "router1" ;;
    redis) echo "redis" ;;
    nats) echo "nats" ;;
    rabbitmq) echo "rabbitmq" ;;
    mosquitto) echo "mosquitto" ;;
    emqx) echo "emqx" ;;
    hivemq) echo "hivemq" ;;
    artemis) echo "artemis" ;;
    rabbitmq-amqp) echo "rabbitmq" ;;
    artemis-amqp) echo "artemis" ;;
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

declare -A STARTED_SERVICES=()

cleanup_processes() {
  log "Cleaning up orphan mq-bench processes..."
  pkill -f "mq-bench.*sub" 2>/dev/null || true
  pkill -f "mq-bench.*pub" 2>/dev/null || true
  sleep 1
}

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

get_standard_port() {
  case "$1" in
    zenoh) echo "7447" ;;
    redis) echo "6379" ;;
    nats) echo "4222" ;;
    rabbitmq) echo "5672" ;;
    rabbitmq-amqp) echo "5672" ;;
    artemis-amqp) echo "5673" ;;
    *) echo "" ;;
  esac
}

# Run a short warmup benchmark (results discarded) to warm broker JIT/caches
run_warmup() {
  local transport="$1" subs="$2"
  local broker_name="${3:-}" broker_host="${4:-}" broker_port="${5:-}"

  if [[ ${WARMUP_SECS} -le 0 ]]; then return 0; fi

  log "Warmup: transport=${transport}${broker_name:+ broker=${broker_name}} subs=${subs} duration=${WARMUP_SECS}s payload=${WARMUP_PAYLOAD}B"

  local env_common="PUBS=${PUBS} SUBS=${subs} RATE=${RATE} PAYLOAD=${WARMUP_PAYLOAD} DURATION=${WARMUP_SECS} SNAPSHOT=${WARMUP_SECS}"
  local host_env=""
  local rid="warmup_$(timestamp)_${transport}"

  case "${transport}" in
    zenoh)
      if [[ -n "${HOST}" ]]; then host_env="ENDPOINT_SUB=tcp/${HOST}:7447 ENDPOINT_PUB=tcp/${HOST}:7447"; fi
      run "ENGINE=zenoh ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true ;;
    redis)
      if [[ -n "${HOST}" ]]; then host_env="REDIS_URL=redis://${HOST}:6379"; fi
      run "ENGINE=redis ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true ;;
    nats)
      if [[ -n "${HOST}" ]]; then host_env="NATS_HOST=${HOST}"; fi
      run "ENGINE=nats ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true ;;
    rabbitmq)
      if [[ -n "${HOST}" ]]; then host_env="RABBITMQ_HOST=${HOST}"; fi
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true ;;
    mqtt)
      if [[ -z "${broker_name}" ]]; then return 0; fi
      host_env="MQTT_HOST=${broker_host} MQTT_PORT=${broker_port}"
      if [[ "${broker_name}" == "artemis" ]]; then
        host_env="${host_env} MQTT_USERNAME=admin MQTT_PASSWORD=admin"
      fi
      run "ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true ;;
    amqp)
      if [[ -z "${broker_name}" ]]; then return 0; fi
      local amqp_user="guest" amqp_pass="guest" amqp_vhost="%2f"
      if [[ "${broker_name}" == "artemis" ]]; then
        amqp_user="admin"; amqp_pass="admin"; amqp_vhost=""
      fi
      local amqp_url="amqp://${amqp_user}:${amqp_pass}@${broker_host}:${broker_port}/${amqp_vhost}"
      host_env="RABBITMQ_URL=${amqp_url}"
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true ;;
  esac

  rm -rf "${REPO_ROOT}/artifacts/${rid}" 2>/dev/null || true
  log "Warmup complete"
}

run_single_execution() {
  local transport="$1" subs="$2" payload="$3" rate="$4"
  local broker_name="${5:-}" broker_host="${6:-}" broker_port="${7:-}"

  local rid_suffix="${transport}_p${payload}_s${subs}_r${rate}"
  if [[ -n "${broker_name}" ]]; then
    rid_suffix="${broker_name}_p${payload}_s${subs}_r${rate}"
  fi

  local rid="${RUN_ID_PREFIX}_$(timestamp)_${rid_suffix}"
  local art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
  local env_common="PUBS=${PUBS} SUBS=${subs} RATE=${rate} PAYLOAD=${payload} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT}"
  local host_env=""
  local stats_pid=""
  local stats_csv="${art_dir}/docker_stats.csv"
  mkdir -p "${art_dir}"

  local stats_duration=$(( RAMP_UP_SECS + DURATION + 15 ))

  # Start stats collector (remote or local)
  if [[ -n "${HOST}" ]]; then
    log "Starting remote stats collector on ${HOST} for ${stats_duration}s..."
    "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${HOST}" "${stats_csv}" "${stats_duration}" >/dev/null 2>&1 &
    stats_pid=$!
  else
    local mon_containers=()
    local svc_name
    if [[ -n "${broker_name}" ]]; then
      svc_name="${broker_name}"
    else
      svc_name="${transport}"
    fi
    resolve_monitor_containers "${svc_name}" mon_containers
    if [[ ${#mon_containers[@]} -gt 0 ]]; then
      log "Starting local stats collector for: ${mon_containers[*]}"
      start_broker_stats_monitor stats_pid "${stats_csv}" "${mon_containers[@]}"
    fi
  fi

  case "${transport}" in
    zenoh)
      if [[ -n "${HOST}" ]]; then host_env="ENDPOINT_SUB=tcp/${HOST}:7447 ENDPOINT_PUB=tcp/${HOST}:7447"; fi
      run "ENGINE=zenoh ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\"" ;;
    redis)
      if [[ -n "${HOST}" ]]; then host_env="REDIS_URL=redis://${HOST}:6379"; fi
      run "ENGINE=redis ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\"" ;;
    nats)
      if [[ -n "${HOST}" ]]; then host_env="NATS_HOST=${HOST}"; fi
      run "ENGINE=nats ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\"" ;;
    rabbitmq)
      if [[ -n "${HOST}" ]]; then host_env="RABBITMQ_HOST=${HOST}"; fi
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\"" ;;
    mqtt)
      if [[ -z "${broker_name}" ]]; then
        log "ERROR: MQTT transport requires broker details"; return 1
      fi
      local br_transport="mqtt_${broker_name}"
      host_env="MQTT_HOST=${broker_host} MQTT_PORT=${broker_port}"
      if [[ "${broker_name}" == "artemis" ]]; then
        host_env="${host_env} MQTT_USERNAME=admin MQTT_PASSWORD=admin"
      fi
      run "ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""

      if [[ -n "${stats_pid}" ]] && (( stats_pid > 0 )); then
        kill "${stats_pid}" 2>/dev/null || true
        wait "${stats_pid}" 2>/dev/null || true
      fi

      append_summary_from_artifacts "${br_transport}" "${payload}" "${subs}" "${rate}" "${rid}" "${art_dir}"
      return 0 ;;
    amqp)
      if [[ -z "${broker_name}" ]]; then
        log "ERROR: AMQP transport requires broker details"; return 1
      fi
      local br_transport="amqp_${broker_name}"
      local amqp_user="guest" amqp_pass="guest" amqp_vhost="%2f"
      if [[ "${broker_name}" == "artemis" ]]; then
        amqp_user="admin"; amqp_pass="admin"; amqp_vhost=""
      fi
      local amqp_url="amqp://${amqp_user}:${amqp_pass}@${broker_host}:${broker_port}/${amqp_vhost}"
      host_env="RABBITMQ_URL=${amqp_url}"
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""

      if [[ -n "${stats_pid}" ]] && (( stats_pid > 0 )); then
        kill "${stats_pid}" 2>/dev/null || true
        wait "${stats_pid}" 2>/dev/null || true
      fi

      append_summary_from_artifacts "${br_transport}" "${payload}" "${subs}" "${rate}" "${rid}" "${art_dir}"
      return 0 ;;
    *) log "Unknown transport: ${transport}"; return 1 ;;
  esac

  # Stop stats collector
  if [[ -n "${stats_pid}" ]] && (( stats_pid > 0 )); then
    kill "${stats_pid}" 2>/dev/null || true
    wait "${stats_pid}" 2>/dev/null || true
  fi

  append_summary_from_artifacts "${transport}" "${payload}" "${subs}" "${rate}" "${rid}" "${art_dir}"
}

main() {
  init_dirs
  log "Fanout benchmark (1 pub → N subs) → ${BENCH_DIR}"

  # Attempt to increase ulimit automatically
  local ulim
  ulim=$(ulimit -n)
  if [[ "$ulim" != "unlimited" ]] && (( ulim < 1048576 )); then
    log "Current ulimit -n is ${ulim}. Attempting to raise to 1048576..."
    if ulimit -n 1048576 2>/dev/null; then
      log "Success: ulimit -n is now $(ulimit -n)"
    else
      log "WARN: Failed to raise ulimit to 1048576. Trying 65536..."
      if ulimit -n 65536 2>/dev/null; then
        log "Success: ulimit -n is now $(ulimit -n)"
      else
        log "WARN: Failed to raise ulimit. Hard limit might be too low. Try 'ulimit -Hn' to check."
      fi
    fi
  fi

  # If NOT sequential, ensure services are up globally (legacy mode)
  if [[ $SEQUENTIAL -eq 0 ]]; then
    ensure_services
  fi

  local payload_bytes="${PAYLOAD_BYTES}"
  log "Resolved: SUMMARY_CSV=${SUMMARY_CSV} | PLOTS_DIR=${PLOTS_DIR} | RAW_DIR=${RAW_DIR}"
  log "Resolved payload (bytes): ${payload_bytes}"
  log "Subs list: ${SUBS_LIST[*]} | pubs=${PUBS} | rate=${RATE}/s"

  for t in "${TRANSPORTS[@]}"; do
    if [[ "${t}" == "mqtt" ]]; then
      if [[ ${#MQTT_BROKERS_ARR[@]} -eq 0 ]]; then
        log "WARN: No MQTT brokers defined; skipping"
      else
        for b in "${MQTT_BROKERS_ARR[@]}"; do
          IFS=: read -r bname bhost bport <<<"${b}"
          local svc=""
          local first_iteration=1

          if [[ $SEQUENTIAL -eq 1 ]]; then
             svc=$(get_services "$bname")
          fi

          for n in "${SUBS_LIST[@]}"; do
            if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
               if [[ $first_iteration -eq 1 ]]; then
                  manage_service up "$svc"
                  first_iteration=0
               else
                  manage_service restart "$svc"
               fi
               wait_for_port "${bhost}" "${bport}"
               run_warmup "mqtt" "${n}" "${bname}" "${bhost}" "${bport}"
            fi

            log "Run: transport=mqtt broker=${bname} subs=${n} payload=${payload_bytes}B rate=${RATE}/s"
            run_single_execution "mqtt" "${n}" "${payload_bytes}" "${RATE}" "${bname}" "${bhost}" "${bport}"

            if [[ ${INTERVAL_SEC} -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
              log "Sleeping ${INTERVAL_SEC}s between runs..."
              sleep "${INTERVAL_SEC}"
            fi
          done

          if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
             manage_service down "$svc"
          fi
        done
      fi
    elif [[ "${t}" == "amqp" ]]; then
      if [[ ${#AMQP_BROKERS_ARR[@]} -eq 0 ]]; then
        log "WARN: No AMQP brokers defined; skipping"
      else
        for b in "${AMQP_BROKERS_ARR[@]}"; do
          IFS=: read -r bname bhost bport <<<"${b}"
          local svc=""
          local first_iteration=1

          if [[ $SEQUENTIAL -eq 1 ]]; then
             svc=$(get_services "${bname}-amqp")
          fi

          for n in "${SUBS_LIST[@]}"; do
            if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
               if [[ $first_iteration -eq 1 ]]; then
                  manage_service up "$svc"
                  first_iteration=0
               else
                  manage_service restart "$svc"
               fi
               wait_for_port "${bhost}" "${bport}"
               run_warmup "amqp" "${n}" "${bname}" "${bhost}" "${bport}"
            fi

            log "Run: transport=amqp broker=${bname} subs=${n} payload=${payload_bytes}B rate=${RATE}/s"
            run_single_execution "amqp" "${n}" "${payload_bytes}" "${RATE}" "${bname}" "${bhost}" "${bport}"

            if [[ ${INTERVAL_SEC} -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
              log "Sleeping ${INTERVAL_SEC}s between runs..."
              sleep "${INTERVAL_SEC}"
            fi
          done

          if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
             manage_service down "$svc"
          fi
        done
      fi
    else
      # Non-MQTT transports
      local svc=""
      local port=""
      local first_iteration=1

      if [[ $SEQUENTIAL -eq 1 ]]; then
         svc=$(get_services "$t")
         port=$(get_standard_port "$t")
      fi

      for n in "${SUBS_LIST[@]}"; do
        if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
           if [[ $first_iteration -eq 1 ]]; then
              manage_service up "$svc"
              first_iteration=0
           else
              manage_service restart "$svc"
           fi
           if [[ -n "$port" ]]; then
              wait_for_port "${HOST:-127.0.0.1}" "$port"
           fi
           run_warmup "${t}" "${n}"
        fi

        log "Run: transport=${t} subs=${n} payload=${payload_bytes}B rate=${RATE}/s"
        run_single_execution "${t}" "${n}" "${payload_bytes}" "${RATE}"

        if [[ ${INTERVAL_SEC} -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
          log "Sleeping ${INTERVAL_SEC}s between runs..."
          sleep "${INTERVAL_SEC}"
        fi
      done

      if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
         manage_service down "$svc"
      fi
    fi
  done

  log "Plotting results to ${PLOTS_DIR}"
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ python3 scripts/plot_results.py --summary ${SUMMARY_CSV} --out-dir ${PLOTS_DIR}"
  else
    python3 "${SCRIPT_DIR}/plot_results.py" --summary "${SUMMARY_CSV}" --out-dir "${PLOTS_DIR}"
  fi

  log "Done. Summary CSV: ${SUMMARY_CSV}"
}

main "$@"
