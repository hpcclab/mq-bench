#!/usr/bin/env bash
set -euo pipefail

# Orchestrate Experiment 1: Fan-Out Under Steady Load.
#
# Research question:
#   How efficiently can a broker replicate messages to many subscribers when
#   publisher count scales slowly with subscriber count?
#
# Topology:
#   P publishers -> S subscribers
#   All publishers publish to the same topic
#   All subscribers subscribe to the same topic
#
# Scaling rule:
#   P = max(MIN_PUBS, ceil(S / SUBS_PER_PUB))
#   Default: 1 publisher per 100 subscribers
#
# Offered load:
#   Publish throughput target  = P * RATE_PER_PUB
#   Delivery throughput target = P * RATE_PER_PUB * S
#
# Runner:
#   Uses scripts/run_fanout.sh for each combination.
#
# Output:
#   results/fanout_steady_load_<ts>/{raw_data,plots}/ with summary.csv.
#
# Usage examples:
#   # Default sweep: 500..5000 subscribers, 1 pub / 100 subs, 10 msg/s per pub
#   scripts/orchestrate_fanout_under_steady_load.sh
#
#   # Custom subscriber list and publish rate per publisher
#   scripts/orchestrate_fanout_under_steady_load.sh --subs-list "500 1000 2000" --rate-per-pub 20
#
#   # Remote host for all brokers
#   scripts/orchestrate_fanout_under_steady_load.sh --host 192.168.0.254
#
#   # Remote broker lifecycle via SSH, starting/stopping broker between runs
#   scripts/orchestrate_fanout_under_steady_load.sh --ssh-target ubuntu@192.168.0.254 --sequential
#
#   # MQTT multi-broker support
#   scripts/orchestrate_fanout_under_steady_load.sh --transports "mqtt" \
#     --mqtt-brokers "mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/lib.sh"

# Defaults
SUBS_LIST=(500 1000 2000 3000 4000 5000 6000 7000)
SUBS_PER_PUB=100
MIN_PUBS=1
RATE_PER_PUB=10
PAYLOAD_TOKEN="1024"
DURATION=30
SNAPSHOT=1
RAMP_UP_SECS=5
WARMUP_SECS=0
WARMUP_PAYLOAD=1024
IGNORE_START_SECS=5
IGNORE_END_SECS=5
RUN_ID_PREFIX="fanout_steady"
TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
DEFAULT_TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
START_SERVICES=0
DRY_RUN=${DRY_RUN:-0}
HOST="${HOST:-}"
SUMMARY_OVERRIDE="${SUMMARY_OVERRIDE:-}"
INTERVAL_SEC=65

# Sequential / Remote execution
SEQUENTIAL=0
SSH_TARGET=""
REMOTE_DIR="~/mq-bench"
APPEND_LATEST=0

# MQTT brokers (name:host:port). Default to local compose ports.
MQTT_BROKERS="mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885 rabbitmq:127.0.0.1:1886 artemis:127.0.0.1:1887"
DEFAULT_MQTT_BROKERS="${MQTT_BROKERS}"
declare -a MQTT_BROKERS_ARR=()

# AMQP brokers (name:host:port). Default to local compose ports.
AMQP_BROKERS="rabbitmq:127.0.0.1:5672"
DEFAULT_AMQP_BROKERS="${AMQP_BROKERS}"
declare -a AMQP_BROKERS_ARR=()

clean_quotes() {
  local v="$1"
  v="${v#[\"\'“”‘’]}"
  v="${v%[\"\'“”‘’]}"
  echo "$v"
}

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
    "\"\""|"''"|"“”"|"””"|"‘‘"|"’’") return 0 ;;
    *) return 1 ;;
  esac
}

timestamp() { date +%Y%m%d_%H%M%S; }
TS=""
BENCH_DIR=""
RAW_DIR=""
PLOTS_DIR=""
SUMMARY_CSV=""

init_dirs() {
  if [[ ${APPEND_LATEST} -eq 1 ]] && [[ -z "${SUMMARY_OVERRIDE}" ]] && [[ -z "${BENCH_DIR}" ]]; then
    local latest_dir
    latest_dir=$(ls -1d "${REPO_ROOT}/results/fanout_steady_load_"* 2>/dev/null | sort -r | head -1 || true)
    if [[ -n "${latest_dir}" ]] && [[ -d "${latest_dir}" ]]; then
      BENCH_DIR="${latest_dir}"
      RAW_DIR="${BENCH_DIR}/raw_data"
      PLOTS_DIR="${BENCH_DIR}/plots"
      TS="${latest_dir##*fanout_steady_load_}"
      log "Appending to existing run: ${BENCH_DIR}"
    else
      log "WARN: --append-latest specified but no existing fanout_steady_load_* directory found. Creating new."
    fi
  fi

  if [[ -z "${TS}" ]]; then TS="$(timestamp)"; fi
  if [[ -z "${BENCH_DIR}" ]]; then BENCH_DIR="${REPO_ROOT}/results/fanout_steady_load_${TS}"; fi
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
      case "${SUMMARY_OVERRIDE}" in
        /*) SUMMARY_CSV="${SUMMARY_OVERRIDE}" ;;
        *) SUMMARY_CSV="${REPO_ROOT}/${SUMMARY_OVERRIDE}" ;;
      esac
      mkdir -p "$(dirname -- "${SUMMARY_CSV}")"
    fi
  else
    SUMMARY_CSV="${RAW_DIR}/summary.csv"
  fi
}

log() { echo "[$(date +%H:%M:%S)] $*"; }
run() { if [[ "${DRY_RUN}" = 1 ]]; then echo "+ $*"; else eval "$*"; fi }

usage() {
  sed -n '1,160p' "$0" | sed -n 's/^# //p'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subs-list)
      shift
      IFS=' ' read -r -a SUBS_LIST <<<"${1:-}"
      ;;
    --subs-per-pub)
      shift
      SUBS_PER_PUB=${1:-100}
      ;;
    --min-pubs)
      shift
      MIN_PUBS=${1:-1}
      ;;
    --rate-per-pub)
      shift
      RATE_PER_PUB=${1:-10}
      ;;
    --payload)
      shift
      PAYLOAD_TOKEN=${1:-1024}
      ;;
    --duration)
      shift
      DURATION=${1:-30}
      ;;
    --snapshot)
      shift
      SNAPSHOT=${1:-1}
      ;;
    --transports)
      shift
      if [[ -n "${1:-}" ]]; then IFS=' ' read -r -a TRANSPORTS <<<"${1}"; fi
      ;;
    --run-id-prefix)
      shift
      RUN_ID_PREFIX=${1:-fanout_steady}
      ;;
    --host)
      shift
      HOST=${1:-}
      ;;
    --start-services)
      START_SERVICES=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    --summary)
      shift
      SUMMARY_OVERRIDE=${1:-}
      if is_quote_only_token "${SUMMARY_OVERRIDE}"; then SUMMARY_OVERRIDE=""; fi
      ;;
    --out-dir)
      shift
      PLOTS_DIR=${1:-}
      if is_quote_only_token "${PLOTS_DIR}"; then PLOTS_DIR=""; fi
      ;;
    --raw-dir)
      shift
      RAW_DIR=${1:-}
      if is_quote_only_token "${RAW_DIR}"; then RAW_DIR=""; fi
      ;;
    --bench-dir)
      shift
      BENCH_DIR=${1:-}
      if is_quote_only_token "${BENCH_DIR}"; then BENCH_DIR=""; fi
      ;;
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
      shift
      INTERVAL_SEC=${1:-0}
      ;;
    --warmup)
      shift
      WARMUP_SECS=${1:-10}
      ;;
    --warmup-payload)
      shift
      WARMUP_PAYLOAD=${1:-1024}
      ;;
    --ignore-start-secs)
      shift
      IGNORE_START_SECS=${1:-5}
      ;;
    --ignore-end-secs)
      shift
      IGNORE_END_SECS=${1:-5}
      ;;
    --sequential)
      SEQUENTIAL=1
      ;;
    --ssh-target)
      shift
      SSH_TARGET=${1:-}
      ;;
    --remote-dir)
      shift
      REMOTE_DIR=${1:-}
      ;;
    --append-latest)
      APPEND_LATEST=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      usage
      exit 2
      ;;
  esac
  shift || true
done

init_dirs

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

PAYLOAD_BYTES="$(to_bytes "${PAYLOAD_TOKEN}")"
if [[ ! "${PAYLOAD_BYTES}" =~ ^[0-9]+$ ]]; then
  echo "[error] Invalid --payload: ${PAYLOAD_TOKEN}" >&2
  exit 2
fi

if [[ ! "${SUBS_PER_PUB}" =~ ^[0-9]+$ ]] || (( SUBS_PER_PUB <= 0 )); then
  echo "[error] Invalid --subs-per-pub: ${SUBS_PER_PUB}" >&2
  exit 2
fi
if [[ ! "${MIN_PUBS}" =~ ^[0-9]+$ ]] || (( MIN_PUBS <= 0 )); then
  echo "[error] Invalid --min-pubs: ${MIN_PUBS}" >&2
  exit 2
fi
if [[ ! "${RATE_PER_PUB}" =~ ^[0-9]+$ ]] || (( RATE_PER_PUB <= 0 )); then
  echo "[error] Invalid --rate-per-pub: ${RATE_PER_PUB}" >&2
  exit 2
fi

if [[ ${#TRANSPORTS[@]} -eq 0 ]]; then TRANSPORTS=("${DEFAULT_TRANSPORTS[@]}"); fi

if [[ -z "${HOST}" ]] && [[ -n "${SSH_TARGET}" ]]; then
  if [[ "${SSH_TARGET}" == *"@"* ]]; then
    HOST="${SSH_TARGET#*@}"
  else
    HOST="${SSH_TARGET}"
  fi
  log "Inferred HOST=${HOST} from SSH_TARGET"
fi

IFS=' ' read -r -a MQTT_BROKERS_ARR <<<"${MQTT_BROKERS}"
declare -a _RESOLVED_BROKERS=()
for tok in "${MQTT_BROKERS_ARR[@]}"; do
  if [[ "${tok}" != *:* ]]; then
    found=""
    for def in ${DEFAULT_MQTT_BROKERS}; do
      IFS=: read -r dname _dhost _dport <<<"${def}"
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
  for tok in "${MQTT_BROKERS_ARR[@]}"; do
    IFS=: read -r bname _bhost bport <<<"${tok}"
    _REWRITTEN+=("${bname}:${HOST}:${bport}")
  done
  MQTT_BROKERS_ARR=("${_REWRITTEN[@]}")
fi

IFS=' ' read -r -a AMQP_BROKERS_ARR <<<"${AMQP_BROKERS}"
declare -a _RESOLVED_AMQP_BROKERS=()
for tok in "${AMQP_BROKERS_ARR[@]}"; do
  if [[ "${tok}" != *:* ]]; then
    found=""
    for def in ${DEFAULT_AMQP_BROKERS}; do
      IFS=: read -r dname _dhost _dport <<<"${def}"
      if [[ "${dname}" == "${tok}" ]]; then
        found="${def}"
        break
      fi
    done
    if [[ -n "${found}" ]]; then
      _RESOLVED_AMQP_BROKERS+=("${found}")
    else
      log "WARN: AMQP broker '${tok}' has no host:port and not found in defaults. Skipping."
    fi
  else
    _RESOLVED_AMQP_BROKERS+=("${tok}")
  fi
done
AMQP_BROKERS_ARR=("${_RESOLVED_AMQP_BROKERS[@]}")

if [[ -n "${HOST}" ]]; then
  declare -a _REWRITTEN_AMQP=()
  for tok in "${AMQP_BROKERS_ARR[@]}"; do
    IFS=: read -r bname _bhost bport <<<"${tok}"
    _REWRITTEN_AMQP+=("${bname}:${HOST}:${bport}")
  done
  AMQP_BROKERS_ARR=("${_REWRITTEN_AMQP[@]}")
fi

if [[ ! -s "${SUMMARY_CSV}" ]]; then
  echo "transport,host,port,payload,subs,pubs,rate_per_pub,rate,delivery_rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,loss_pct,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes,avg_cpu_perc,avg_mem_perc,avg_mem_used_bytes,max_net_rx_bps,max_net_tx_bps,avg_net_rx_bps,avg_net_tx_bps" > "${SUMMARY_CSV}"
fi

calc_pubs_for_subs() {
  local subs="$1"
  local pubs=$(( (subs + SUBS_PER_PUB - 1) / SUBS_PER_PUB ))
  if (( pubs < MIN_PUBS )); then pubs="${MIN_PUBS}"; fi
  echo "${pubs}"
}

extract_steady_state_metrics() {
  local sub_csv="$1"
  awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
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
      if (duration > 0) tps = (last_recv - first_recv) / duration
      else tps = 0
      avg_p50 = sum_p50 / count
      avg_p95 = sum_p95 / count
      avg_p99 = sum_p99 / count
      delta_recv = last_recv - first_recv
      delta_sent = last_sent - first_sent
      delta_err = last_err - first_err
      printf "%.2f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%d,%d,%d", tps, avg_p50, avg_p95, avg_p99, delta_sent, delta_recv, delta_err, count, first_ts, last_ts
    }
  ' "${sub_csv}"
}

extract_pub_tps() {
  local pub_csv="$1"
  awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
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
          last_ts = ts
          last_sent = row_sent[i]
        }
      }
      duration = last_ts - first_ts
      if (duration > 0) printf "%.2f", (last_sent - first_sent) / duration
      else print ""
    }
  ' "${pub_csv}"
}

extract_stats_metrics() {
  local stats_csv="$1"
  local steady_start_ts="$2"
  local steady_end_ts="$3"
  awk -F, -v start_ts="${steady_start_ts}" -v end_ts="${steady_end_ts}" '
    function parse_percent(s) {
      gsub(/%/, "", s)
      return s + 0
    }
    function parse_bytes_token(raw, val, unit) {
      raw = raw ""
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", raw)
      if (raw == "" || raw == "-") return 0
      val = raw + 0
      unit = raw
      gsub(/[0-9.]/, "", unit)
      if (unit == "" || unit == "B") return val
      if (unit == "kB" || unit == "KB") return val * 1000
      if (unit == "MB") return val * 1000 * 1000
      if (unit == "GB") return val * 1000 * 1000 * 1000
      if (unit == "TB") return val * 1000 * 1000 * 1000 * 1000
      if (unit == "KiB") return val * 1024
      if (unit == "MiB") return val * 1024 * 1024
      if (unit == "GiB") return val * 1024 * 1024 * 1024
      if (unit == "TiB") return val * 1024 * 1024 * 1024 * 1024
      return val
    }
    function parse_mem_used(mem_usage, parts, used_part, n) {
      n = split(mem_usage, parts, "/")
      used_part = (n >= 1 ? parts[1] : "")
      return parse_bytes_token(used_part)
    }
    function parse_mem_total(mem_usage, parts, total_part, n) {
      n = split(mem_usage, parts, "/")
      total_part = (n >= 2 ? parts[2] : "")
      return parse_bytes_token(total_part)
    }
    function is_valid_stats_sample(cpu, used_b, tot_b, rx_b, tx_b) {
      return (tot_b > 0 || used_b > 0 || cpu > 0 || rx_b > 0 || tx_b > 0)
    }
    function finish_group(   dt, drx, dtx, rx_rate, tx_rate) {
      if (group_ts == "") return
      if (prev_ts != "" && group_ts > prev_ts && have_prev_net && group_has_net) {
        dt = group_ts - prev_ts
        drx = group_rx - prev_rx
        dtx = group_tx - prev_tx
        # Treat counter resets or container restarts as discontinuities.
        if (drx < 0 || dtx < 0) {
          prev_ts = group_ts
          prev_rx = group_rx
          prev_tx = group_tx
          have_prev_net = group_has_net
          return
        }
        rx_rate = (drx * 8.0) / dt
        tx_rate = (dtx * 8.0) / dt
        if (rx_rate > max_rx_bps) max_rx_bps = rx_rate
        if (tx_rate > max_tx_bps) max_tx_bps = tx_rate
        sum_rx_bps += rx_rate
        sum_tx_bps += tx_rate
        net_samples++
      }
      prev_ts = group_ts
      prev_rx = group_rx
      prev_tx = group_tx
      have_prev_net = group_has_net
    }
    NR == 1 {
      fmt = "unknown"
      if (NF >= 17) fmt = "local"
      else if (NF >= 7 && $3 == "cpu_perc") fmt = "remote_ext"
      else if (NF == 5 && $3 == "cpu_perc") fmt = "remote_legacy"
      next
    }
    {
      ts = $1 + 0
      if (start_ts > 0 && end_ts > 0) {
        if (ts < start_ts || ts > end_ts) next
      }

      cpu = 0
      mem_perc = 0
      used_b = 0
      tot_b = 0
      has_net = 0
      rx_b = 0
      tx_b = 0

      if (fmt == "local") {
        cpu = ($17 != "" ? $17 + 0 : parse_percent($4))
        used_b = $10 + 0
        tot_b = $11 + 0
        mem_perc = $12 + 0
        if (mem_perc == 0 && tot_b > 0) mem_perc = (used_b / tot_b) * 100.0
        rx_b = $13 + 0
        tx_b = $14 + 0
        has_net = 1
      } else if (fmt == "remote_ext") {
        cpu = parse_percent($3)
        mem_perc = parse_percent($4)
        used_b = parse_mem_used($5)
        tot_b = parse_mem_total($5)
        rx_b = $6 + 0
        tx_b = $7 + 0
        has_net = 1
      } else if (fmt == "remote_legacy") {
        cpu = parse_percent($3)
        mem_perc = parse_percent($4)
        used_b = parse_mem_used($5)
        tot_b = parse_mem_total($5)
      } else {
        next
      }

      if (!is_valid_stats_sample(cpu, used_b, tot_b, rx_b, tx_b)) next

      if (cpu > max_cpu) max_cpu = cpu
      if (mem_perc > max_mem_perc) max_mem_perc = mem_perc
      if (used_b > max_mem_used) max_mem_used = used_b
      sum_cpu += cpu
      sum_mem_perc += mem_perc
      sum_mem_used += used_b
      samples++

      if (has_net) {
        if (group_ts == "") {
          group_ts = ts
          group_rx = 0
          group_tx = 0
          group_has_net = 0
        }
        if (ts != group_ts) {
          finish_group()
          group_ts = ts
          group_rx = 0
          group_tx = 0
          group_has_net = 0
        }
        group_rx += rx_b
        group_tx += tx_b
        group_has_net = 1
      }
    }
    END {
      finish_group()
      if (samples > 0) {
        avg_cpu = sum_cpu / samples
        avg_mem_perc = sum_mem_perc / samples
        avg_mem_used = sum_mem_used / samples
      } else {
        max_cpu = 0
        max_mem_perc = 0
        max_mem_used = 0
        avg_cpu = 0
        avg_mem_perc = 0
        avg_mem_used = 0
      }
      if (net_samples > 0) {
        avg_rx_bps = sum_rx_bps / net_samples
        avg_tx_bps = sum_tx_bps / net_samples
      } else {
        max_rx_bps = 0
        max_tx_bps = 0
        avg_rx_bps = 0
        avg_tx_bps = 0
      }
      printf "%.6f,%.6f,%.0f,%.6f,%.6f,%.0f,%.6f,%.6f,%.6f,%.6f,%d,%d", max_cpu, max_mem_perc, max_mem_used, avg_cpu, avg_mem_perc, avg_mem_used, max_rx_bps, max_tx_bps, avg_rx_bps, avg_tx_bps, samples, net_samples
    }
  ' "${stats_csv}" || true
}

append_summary_from_artifacts() {
  local transport="$1"
  local host="$2"
  local port="$3"
  local payload="$4"
  local subs="$5"
  local pubs="$6"
  local rate_per_pub="$7"
  local total_rate="$8"
  local delivery_rate="$9"
  local run_id="${10}"
  local art_dir="${11}"

  local sub_csv="${art_dir}/sub_agg.csv"
  local pub_csv="${art_dir}/pub_agg.csv"
  if [[ ! -f "${sub_csv}" ]]; then
    if [[ -f "${art_dir}/sub.csv" ]]; then
      sub_csv="${art_dir}/sub.csv"
    else
      log "WARN: Missing subscriber CSV in ${art_dir}"
      return 0
    fi
  fi
  if [[ ! -f "${pub_csv}" ]] && [[ -f "${art_dir}/pub.csv" ]]; then
    pub_csv="${art_dir}/pub.csv"
  fi

  local steady_state_metrics
  steady_state_metrics="$(extract_steady_state_metrics "${sub_csv}")"
  if [[ -z "${steady_state_metrics}" ]]; then
    log "WARN: No subscriber data for ${run_id}"
    return 0
  fi

  local tps avg_p50_ns avg_p95_ns avg_p99_ns sent recv errors steady_rows steady_start_ts steady_end_ts
  IFS=, read -r tps avg_p50_ns avg_p95_ns avg_p99_ns sent recv errors steady_rows steady_start_ts steady_end_ts <<<"${steady_state_metrics}"

  if [[ "${steady_rows}" -eq 0 ]]; then
    log "WARN: No steady-state rows found for ${run_id} (ignore_start=${IGNORE_START_SECS}s, ignore_end=${IGNORE_END_SECS}s)"
    return 0
  fi

  local loss_pct
  loss_pct=$(awk -v s="${sent}" -v r="${recv}" 'BEGIN{if(s>0){printf("%.2f", (s-r)/s*100)}else{print "0.00"}}')

  local pub_tps=""
  if [[ -f "${pub_csv}" ]]; then
    pub_tps="$(extract_pub_tps "${pub_csv}")"
  fi

  local p50_ms p95_ms p99_ms
  p50_ms=$(awk -v n="${avg_p50_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')
  p95_ms=$(awk -v n="${avg_p95_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')
  p99_ms=$(awk -v n="${avg_p99_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')

  local stats_csv="${art_dir}/docker_stats.csv"
  local max_cpu="" max_mem_perc="" max_mem_used="" avg_cpu="" avg_mem_perc="" avg_mem_used=""
  local max_net_rx_bps="" max_net_tx_bps="" avg_net_rx_bps="" avg_net_tx_bps=""
  if [[ -f "${stats_csv}" ]]; then
    local agg
    agg="$(extract_stats_metrics "${stats_csv}" "${steady_start_ts}" "${steady_end_ts}")"
    local stats_rows net_rows
    IFS=, read -r max_cpu max_mem_perc max_mem_used avg_cpu avg_mem_perc avg_mem_used max_net_rx_bps max_net_tx_bps avg_net_rx_bps avg_net_tx_bps stats_rows net_rows <<<"${agg}"
    if [[ "${stats_rows:-0}" -eq 0 ]]; then
      max_cpu=""; max_mem_perc=""; max_mem_used=""
      avg_cpu=""; avg_mem_perc=""; avg_mem_used=""
    fi
    if [[ "${net_rows:-0}" -eq 0 ]]; then
      max_net_rx_bps=""; max_net_tx_bps=""
      avg_net_rx_bps=""; avg_net_tx_bps=""
    fi
  fi

  echo "${transport},${host},${port},${payload},${subs},${pubs},${rate_per_pub},${total_rate},${delivery_rate},${run_id},${tps},${p50_ms},${p95_ms},${p99_ms},${pub_tps},${sent},${recv},${errors},${loss_pct},${art_dir},${max_cpu},${max_mem_perc},${max_mem_used},${avg_cpu},${avg_mem_perc},${avg_mem_used},${max_net_rx_bps},${max_net_tx_bps},${avg_net_rx_bps},${avg_net_tx_bps}" >> "${SUMMARY_CSV}"
}

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

docker_compose_cmd() {
  local action="$1"
  local services="${2:-}"
  local cmd="docker compose ${action}"
  if [[ -n "${services}" ]]; then cmd="${cmd} ${services}"; fi

  if [[ -n "${SSH_TARGET}" ]]; then
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
  local start_ts
  start_ts=$(date +%s)
  while true; do
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; then
      log "Port ${host}:${port} is open."
      return 0
    fi
    local now_ts
    now_ts=$(date +%s)
    if (( now_ts - start_ts > timeout )); then
      log "Timeout waiting for ${host}:${port}"
      return 1
    fi
    sleep 1
  done
}

cleanup_processes() {
  log "Cleaning up orphan mq-bench processes..."
  pkill -f "mq-bench.*sub" 2>/dev/null || true
  pkill -f "mq-bench.*pub" 2>/dev/null || true
  sleep 1
}

manage_service() {
  local action="$1"
  local services="$2"
  if [[ ${SEQUENTIAL} -eq 0 ]]; then return 0; fi
  if [[ -z "${services}" ]]; then return 0; fi

  if [[ "${action}" == "up" ]]; then
    log "Starting services: ${services}"
    cleanup_processes
    docker_compose_cmd down
    if [[ "${DRY_RUN}" != 1 ]]; then
      log "Waiting 10s for connections to settle..."
      sleep 10
    fi
    docker_compose_cmd "up -d" "${services}"
    if [[ "${DRY_RUN}" != 1 ]]; then sleep 5; fi
  elif [[ "${action}" == "restart" ]]; then
    log "Full restarting services: ${services}"
    cleanup_processes
    docker_compose_cmd down
    if [[ "${DRY_RUN}" != 1 ]]; then
      log "Waiting 10s for connections to settle..."
      sleep 10
    fi
    docker_compose_cmd "up -d" "${services}"
    if [[ "${DRY_RUN}" != 1 ]]; then sleep 5; fi
  elif [[ "${action}" == "down" ]]; then
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
    mqtt) echo "1883" ;;
    *) echo "" ;;
  esac
}

run_warmup() {
  local transport="$1"
  local subs="$2"
  local pubs="$3"
  local total_rate="$4"
  local broker_name="${5:-}"
  local broker_host="${6:-}"
  local broker_port="${7:-}"

  if [[ ${WARMUP_SECS} -le 0 ]]; then return 0; fi

  log "Warmup: transport=${transport}${broker_name:+ broker=${broker_name}} subs=${subs} pubs=${pubs} duration=${WARMUP_SECS}s payload=${WARMUP_PAYLOAD}B"

  local env_common="PUBS=${pubs} SUBS=${subs} RATE=${total_rate} PAYLOAD=${WARMUP_PAYLOAD} DURATION=${WARMUP_SECS} SNAPSHOT=${WARMUP_SECS}"
  local host_env=""
  local rid="warmup_$(timestamp)_${transport}"

  case "${transport}" in
    zenoh)
      if [[ -n "${HOST}" ]]; then host_env="ENDPOINT_SUB=tcp/${HOST}:7447 ENDPOINT_PUB=tcp/${HOST}:7447"; fi
      run "ENGINE=zenoh ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true
      ;;
    redis)
      if [[ -n "${HOST}" ]]; then host_env="REDIS_URL=redis://${HOST}:6379"; fi
      run "ENGINE=redis ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true
      ;;
    nats)
      if [[ -n "${HOST}" ]]; then host_env="NATS_HOST=${HOST}"; fi
      run "ENGINE=nats ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true
      ;;
    rabbitmq)
      if [[ -n "${HOST}" ]]; then host_env="RABBITMQ_HOST=${HOST}"; fi
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true
      ;;
    mqtt)
      if [[ -z "${broker_name}" ]]; then return 0; fi
      host_env="MQTT_HOST=${broker_host} MQTT_PORT=${broker_port}"
      if [[ "${broker_name}" == "artemis" ]]; then
        host_env="${host_env} MQTT_USERNAME=admin MQTT_PASSWORD=admin"
      fi
      run "ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true
      ;;
    amqp)
      if [[ -z "${broker_name}" ]]; then return 0; fi
      local amqp_user="guest" amqp_pass="guest" amqp_vhost="%2f"
      if [[ "${broker_name}" == "artemis" ]]; then
        amqp_user="admin"; amqp_pass="admin"; amqp_vhost=""
      fi
      local amqp_url="amqp://${amqp_user}:${amqp_pass}@${broker_host}:${broker_port}/${amqp_vhost}"
      host_env="RABBITMQ_URL=${amqp_url}"
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\" >/dev/null 2>&1" || true
      ;;
  esac

  rm -rf "${REPO_ROOT}/artifacts/${rid}" 2>/dev/null || true
  log "Warmup complete"
}

is_remote_host() {
  local host="$1"
  [[ -n "${host}" ]] && [[ "${host}" != "127.0.0.1" ]] && [[ "${host}" != "localhost" ]]
}

get_remote_stats_target() {
  local broker_host="${1:-}"
  if [[ -n "${SSH_TARGET}" ]]; then
    if [[ -z "${broker_host}" || "${broker_host}" == "${HOST}" ]]; then
      echo "${SSH_TARGET}"
      return 0
    fi
  fi
  echo "${broker_host:-${HOST}}"
}

run_single_execution() {
  local transport="$1"
  local subs="$2"
  local pubs="$3"
  local payload="$4"
  local rate_per_pub="$5"
  local total_rate="$6"
  local broker_name="${7:-}"
  local broker_host="${8:-}"
  local broker_port="${9:-}"

  local delivery_rate=$(( total_rate * subs ))
  local rid_suffix="${transport}_p${payload}_s${subs}_u${pubs}_r${rate_per_pub}"
  if [[ -n "${broker_name}" ]]; then
    rid_suffix="${broker_name}_p${payload}_s${subs}_u${pubs}_r${rate_per_pub}"
  fi

  local rid="${RUN_ID_PREFIX}_$(timestamp)_${rid_suffix}"
  local art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
  local env_common="PUBS=${pubs} SUBS=${subs} RATE=${total_rate} PAYLOAD=${payload} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT}"
  local host_env=""
  local stats_pid=""
  local stats_csv="${art_dir}/docker_stats.csv"
  local summary_transport="${transport}"
  local summary_host="${broker_host:-${HOST:-127.0.0.1}}"
  local summary_port="${broker_port:-}"
  local stats_duration=$(( RAMP_UP_SECS + DURATION + 15 ))
  local stats_container=""
  local monitor_env=""
  mkdir -p "${art_dir}"

  if [[ -n "${broker_name}" ]]; then
    if [[ "${transport}" == "mqtt" ]]; then
      summary_transport="mqtt_${broker_name}"
      stats_container="$(get_services "${broker_name}")"
    elif [[ "${transport}" == "amqp" ]]; then
      summary_transport="amqp_${broker_name}"
      stats_container="$(get_services "${broker_name}-amqp")"
    fi
  else
    stats_container="$(get_services "${transport}")"
  fi

  if [[ -z "${summary_port}" ]]; then
    if [[ "${transport}" == "amqp" ]] && [[ -n "${broker_name}" ]]; then
      summary_port="$(get_standard_port "${broker_name}-amqp")"
    else
      summary_port="$(get_standard_port "${transport}")"
    fi
  fi

  if ! is_remote_host "${summary_host}" && [[ -n "${stats_container}" ]]; then
    monitor_env="MONITOR_CONTAINERS=${stats_container}"
  fi

  local remote_stats_target
  remote_stats_target="$(get_remote_stats_target "${broker_host}")"
  if [[ "${DRY_RUN}" != 1 ]] && is_remote_host "${summary_host}"; then
    log "Starting remote stats collector on ${remote_stats_target} for ${stats_duration}s..."
    if [[ -n "${stats_container}" ]]; then
      "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${remote_stats_target}" "${stats_csv}" "${stats_duration}" --extended --containers "${stats_container}" >/dev/null 2>&1 &
    else
      "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${remote_stats_target}" "${stats_csv}" "${stats_duration}" --extended >/dev/null 2>&1 &
    fi
    stats_pid=$!
  fi

  case "${transport}" in
    zenoh)
      if [[ -n "${HOST}" ]]; then host_env="ENDPOINT_SUB=tcp/${HOST}:7447 ENDPOINT_PUB=tcp/${HOST}:7447"; fi
      run "${monitor_env} ENGINE=zenoh ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      ;;
    redis)
      if [[ -n "${HOST}" ]]; then host_env="REDIS_URL=redis://${HOST}:6379"; fi
      run "${monitor_env} ENGINE=redis ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      ;;
    nats)
      if [[ -n "${HOST}" ]]; then host_env="NATS_HOST=${HOST}"; fi
      run "${monitor_env} ENGINE=nats ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      ;;
    rabbitmq)
      if [[ -n "${HOST}" ]]; then host_env="RABBITMQ_HOST=${HOST}"; fi
      run "${monitor_env} ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      ;;
    mqtt)
      if [[ -z "${broker_name}" ]]; then
        log "ERROR: MQTT transport requires broker details"
        return 1
      fi
      host_env="MQTT_HOST=${broker_host} MQTT_PORT=${broker_port}"
      if [[ "${broker_name}" == "artemis" ]]; then
        host_env="${host_env} MQTT_USERNAME=admin MQTT_PASSWORD=admin"
      fi
      run "${monitor_env} ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      ;;
    amqp)
      if [[ -z "${broker_name}" ]]; then
        log "ERROR: AMQP transport requires broker details"
        return 1
      fi
      local amqp_user="guest" amqp_pass="guest" amqp_vhost="%2f"
      if [[ "${broker_name}" == "artemis" ]]; then
        amqp_user="admin"; amqp_pass="admin"; amqp_vhost=""
      fi
      local amqp_url="amqp://${amqp_user}:${amqp_pass}@${broker_host}:${broker_port}/${amqp_vhost}"
      host_env="RABBITMQ_URL=${amqp_url}"
      run "${monitor_env} ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      ;;
    *)
      log "Unknown transport: ${transport}"
      return 1
      ;;
  esac

  if [[ -n "${stats_pid}" ]] && (( stats_pid > 0 )); then
    kill "${stats_pid}" 2>/dev/null || true
    wait "${stats_pid}" 2>/dev/null || true
  fi

  if [[ "${DRY_RUN}" = 1 ]]; then
    return 0
  fi

  append_summary_from_artifacts "${summary_transport}" "${summary_host}" "${summary_port}" "${payload}" "${subs}" "${pubs}" "${rate_per_pub}" "${total_rate}" "${delivery_rate}" "${rid}" "${art_dir}"
}

main() {
  init_dirs
  log "Fan-out steady-load benchmark -> ${BENCH_DIR}"

  local ulim
  ulim=$(ulimit -n)
  if [[ "${ulim}" != "unlimited" ]] && (( ulim < 1048576 )); then
    log "Current ulimit -n is ${ulim}. Attempting to raise to 1048576..."
    if ulimit -n 1048576 2>/dev/null; then
      log "Success: ulimit -n is now $(ulimit -n)"
    else
      log "WARN: Failed to raise ulimit to 1048576. Trying 65536..."
      if ulimit -n 65536 2>/dev/null; then
        log "Success: ulimit -n is now $(ulimit -n)"
      else
        log "WARN: Failed to raise ulimit. Hard limit might be too low."
      fi
    fi
  fi

  if [[ ${SEQUENTIAL} -eq 0 ]]; then
    ensure_services
  fi

  local payload_bytes="${PAYLOAD_BYTES}"
  log "Resolved: SUMMARY_CSV=${SUMMARY_CSV} | PLOTS_DIR=${PLOTS_DIR} | RAW_DIR=${RAW_DIR}"
  log "Resolved payload (bytes): ${payload_bytes}"
  log "Subscriber sweep: ${SUBS_LIST[*]}"
  log "Publisher rule: pubs=max(${MIN_PUBS}, ceil(subs/${SUBS_PER_PUB})) | rate_per_pub=${RATE_PER_PUB}/s"

  for t in "${TRANSPORTS[@]}"; do
    if [[ "${t}" == "mqtt" ]]; then
      if [[ ${#MQTT_BROKERS_ARR[@]} -eq 0 ]]; then
        log "WARN: No MQTT brokers defined; skipping"
      else
        for b in "${MQTT_BROKERS_ARR[@]}"; do
          IFS=: read -r bname bhost bport <<<"${b}"
          local svc=""
          local first_iteration=1
          if [[ ${SEQUENTIAL} -eq 1 ]]; then svc="$(get_services "${bname}")"; fi

          for n in "${SUBS_LIST[@]}"; do
            local pubs total_rate delivery_rate
            pubs="$(calc_pubs_for_subs "${n}")"
            total_rate=$(( pubs * RATE_PER_PUB ))
            delivery_rate=$(( total_rate * n ))

            if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
              if [[ ${first_iteration} -eq 1 ]]; then
                manage_service up "${svc}"
                first_iteration=0
              else
                manage_service restart "${svc}"
              fi
              wait_for_port "${bhost}" "${bport}"
              run_warmup "mqtt" "${n}" "${pubs}" "${total_rate}" "${bname}" "${bhost}" "${bport}"
            fi

            log "Run: transport=mqtt broker=${bname} subs=${n} pubs=${pubs} payload=${payload_bytes}B publish_rate=${total_rate}/s delivery_rate=${delivery_rate}/s"
            run_single_execution "mqtt" "${n}" "${pubs}" "${payload_bytes}" "${RATE_PER_PUB}" "${total_rate}" "${bname}" "${bhost}" "${bport}"

            if [[ ${INTERVAL_SEC} -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
              log "Sleeping ${INTERVAL_SEC}s between runs..."
              sleep "${INTERVAL_SEC}"
            fi
          done

          if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
            manage_service down "${svc}"
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
          if [[ ${SEQUENTIAL} -eq 1 ]]; then svc="$(get_services "${bname}-amqp")"; fi

          for n in "${SUBS_LIST[@]}"; do
            local pubs total_rate delivery_rate
            pubs="$(calc_pubs_for_subs "${n}")"
            total_rate=$(( pubs * RATE_PER_PUB ))
            delivery_rate=$(( total_rate * n ))

            if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
              if [[ ${first_iteration} -eq 1 ]]; then
                manage_service up "${svc}"
                first_iteration=0
              else
                manage_service restart "${svc}"
              fi
              wait_for_port "${bhost}" "${bport}"
              run_warmup "amqp" "${n}" "${pubs}" "${total_rate}" "${bname}" "${bhost}" "${bport}"
            fi

            log "Run: transport=amqp broker=${bname} subs=${n} pubs=${pubs} payload=${payload_bytes}B publish_rate=${total_rate}/s delivery_rate=${delivery_rate}/s"
            run_single_execution "amqp" "${n}" "${pubs}" "${payload_bytes}" "${RATE_PER_PUB}" "${total_rate}" "${bname}" "${bhost}" "${bport}"

            if [[ ${INTERVAL_SEC} -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
              log "Sleeping ${INTERVAL_SEC}s between runs..."
              sleep "${INTERVAL_SEC}"
            fi
          done

          if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
            manage_service down "${svc}"
          fi
        done
      fi
    else
      local svc=""
      local port=""
      local first_iteration=1
      if [[ ${SEQUENTIAL} -eq 1 ]]; then
        svc="$(get_services "${t}")"
        port="$(get_standard_port "${t}")"
      fi

      for n in "${SUBS_LIST[@]}"; do
        local pubs total_rate delivery_rate
        pubs="$(calc_pubs_for_subs "${n}")"
        total_rate=$(( pubs * RATE_PER_PUB ))
        delivery_rate=$(( total_rate * n ))

        if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
          if [[ ${first_iteration} -eq 1 ]]; then
            manage_service up "${svc}"
            first_iteration=0
          else
            manage_service restart "${svc}"
          fi
          if [[ -n "${port}" ]]; then
            wait_for_port "${HOST:-127.0.0.1}" "${port}"
          fi
          run_warmup "${t}" "${n}" "${pubs}" "${total_rate}"
        fi

        log "Run: transport=${t} subs=${n} pubs=${pubs} payload=${payload_bytes}B publish_rate=${total_rate}/s delivery_rate=${delivery_rate}/s"
        run_single_execution "${t}" "${n}" "${pubs}" "${payload_bytes}" "${RATE_PER_PUB}" "${total_rate}"

        if [[ ${INTERVAL_SEC} -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
          log "Sleeping ${INTERVAL_SEC}s between runs..."
          sleep "${INTERVAL_SEC}"
        fi
      done

      if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
        manage_service down "${svc}"
      fi
    fi
  done

  log "Plotting results to ${PLOTS_DIR}"
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ python3 ${SCRIPT_DIR}/plot_results.py --summary ${SUMMARY_CSV} --out-dir ${PLOTS_DIR}"
  else
    python3 "${SCRIPT_DIR}/plot_results.py" --summary "${SUMMARY_CSV}" --out-dir "${PLOTS_DIR}"
  fi

  log "Done. Summary CSV: ${SUMMARY_CSV}"
}

main "$@"
