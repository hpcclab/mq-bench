#!/usr/bin/env bash
set -euo pipefail

# Orchestrate Experiment 2: Fan-In Under Steady Load.
#
# Research question:
#   How does broker performance change when many publishers send to one shared
#   topic instead of isolated 1-to-1 pairs?
#
# Topology:
#   N publishers -> 1 subscriber
#   All publishers publish to the same topic
#   The subscriber subscribes to that same topic
#
# Default parameters from docs/MQ_Bench_Journal_Extension copy.pptx:
#   Publishers: 500, 1000, 2000, 5000, 10000
#   Subscribers: 1
#   Rate per publisher: 10 msg/s
#   Payload: 1 KB
#   Duration: 120 s
#   Warmup: 60 s
#   QoS: 0
#
# Output:
#   results/fanin_steady_load_<ts>/{raw_data,plots}/ with summary.csv.
#   plots/ contains PNG graphs and plots/latex/ contains LaTeX-ready PDF graphs.
#
# Usage examples:
#   scripts/orchestrate_fanin_under_steady_load.sh
#   scripts/orchestrate_fanin_under_steady_load.sh --publishers-list "500 1000" --transports "zenoh nats"
#   scripts/orchestrate_fanin_under_steady_load.sh --host 192.168.0.254 --transports "redis nats"
#   scripts/orchestrate_fanin_under_steady_load.sh --ssh-target ubuntu@192.168.0.254 --sequential --remote-dir /home/ubuntu/mq-bench
#   scripts/orchestrate_fanin_under_steady_load.sh --transports "mqtt" --mqtt-brokers "mosquitto emqx"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/lib.sh"

PUBLISHERS_LIST=(500 1000 2000 5000 10000)
SUBSCRIBERS=1
RATE_PER_PUB=10
PAYLOAD_TOKEN="1024"
DURATION=120
WARMUP_SECS=60
SNAPSHOT=1
QOS=0
KEY="bench/topic"
SUB_STARTUP_DELAY=5
IGNORE_START_SECS=5
IGNORE_END_SECS=5
RUN_ID_PREFIX="fanin_steady"
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
WARMUP_PAYLOAD=""

MQTT_BROKERS="mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885 rabbitmq:127.0.0.1:1886 artemis:127.0.0.1:1887"
DEFAULT_MQTT_BROKERS="${MQTT_BROKERS}"
declare -a MQTT_BROKERS_ARR=()

AMQP_BROKERS="rabbitmq:127.0.0.1:5672"
DEFAULT_AMQP_BROKERS="${AMQP_BROKERS}"
declare -a AMQP_BROKERS_ARR=()

TS=""
BENCH_DIR=""
RAW_DIR=""
PLOTS_DIR=""
SUMMARY_CSV=""
PAYLOAD_BYTES=""
BIN="./target/release/mq-bench"

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
log() { echo "[$(date +%H:%M:%S)] $*"; }
run() { if [[ "${DRY_RUN}" = 1 ]]; then echo "+ $*"; else eval "$*"; fi }

usage() {
  sed -n '1,120p' "$0" | sed -n 's/^# //p'
}

init_dirs() {
  if [[ ${APPEND_LATEST} -eq 1 ]] && [[ -z "${SUMMARY_OVERRIDE}" ]] && [[ -z "${BENCH_DIR}" ]]; then
    local latest_dir
    latest_dir=$(ls -1d "${REPO_ROOT}/results/fanin_steady_load_"* 2>/dev/null | sort -r | head -1 || true)
    if [[ -n "${latest_dir}" ]] && [[ -d "${latest_dir}" ]]; then
      BENCH_DIR="${latest_dir}"
      RAW_DIR="${BENCH_DIR}/raw_data"
      PLOTS_DIR="${BENCH_DIR}/plots"
      TS="${latest_dir##*fanin_steady_load_}"
      log "Appending to existing run: ${BENCH_DIR}"
    else
      log "WARN: --append-latest specified but no existing fanin_steady_load_* directory found. Creating new."
    fi
  fi

  if [[ -z "${TS}" ]]; then TS="$(timestamp)"; fi
  if [[ -z "${BENCH_DIR}" ]]; then BENCH_DIR="${REPO_ROOT}/results/fanin_steady_load_${TS}"; fi
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

docker_compose_cmd() {
  local action="$1"
  local services="${2:-}"
  local cmd="docker compose ${action}"
  if [[ -n "${services}" ]]; then cmd="${cmd} ${services}"; fi

  if [[ -n "${SSH_TARGET}" ]]; then
    local rcmd="cd ${REMOTE_DIR} && ${cmd}"
    log "[remote] ${SSH_TARGET}: ${rcmd}"
    if [[ "${DRY_RUN}" = 1 ]]; then
      echo "+ ssh ${SSH_TARGET} "${rcmd}""
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
  if [[ "${DRY_RUN}" = 1 ]]; then
    log "[dry-run] Would wait for ${host}:${port}"
    return 0
  fi
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
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ pkill -f 'mq-bench.*sub'"
    echo "+ pkill -f 'mq-bench.*pub'"
    return 0
  fi
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

write_summary_header() {
  if [[ ! -s "${SUMMARY_CSV}" ]]; then
    echo "transport,host,port,payload,subs,pubs,rate_per_pub,rate,delivery_rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,loss_pct,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes,avg_cpu_perc,avg_mem_perc,avg_mem_used_bytes,max_net_rx_bps,max_net_tx_bps,avg_net_rx_bps,avg_net_tx_bps" > "${SUMMARY_CSV}"
  fi
}

build_release_if_stale() {
  local bin="${1:-./target/release/mq-bench}"
  if [[ ! -x "${bin}" ]]; then
    echo "[build] Building release binary..."
    cargo build --release
    return 0
  fi
  if find src Cargo.toml Cargo.lock -type f -newer "${bin}" -print -quit | grep -q .; then
    echo "[build] Source is newer than ${bin}; rebuilding release binary..."
    cargo build --release
  fi
}

resolve_broker_lists() {
  IFS=' ' read -r -a MQTT_BROKERS_ARR <<<"${MQTT_BROKERS}"
  declare -a resolved_mqtt=()
  for tok in "${MQTT_BROKERS_ARR[@]}"; do
    if [[ "${tok}" != *:* ]]; then
      local found=""
      for def in ${DEFAULT_MQTT_BROKERS}; do
        IFS=: read -r dname _dhost _dport <<<"${def}"
        if [[ "${dname}" == "${tok}" ]]; then found="${def}"; break; fi
      done
      if [[ -n "${found}" ]]; then
        resolved_mqtt+=("${found}")
      else
        log "WARN: MQTT broker '${tok}' has no host:port and was not found in defaults. Skipping."
      fi
    else
      resolved_mqtt+=("${tok}")
    fi
  done
  MQTT_BROKERS_ARR=("${resolved_mqtt[@]}")

  if [[ -n "${HOST}" ]]; then
    declare -a rewritten_mqtt=()
    for tok in "${MQTT_BROKERS_ARR[@]}"; do
      IFS=: read -r bname _bhost bport <<<"${tok}"
      rewritten_mqtt+=("${bname}:${HOST}:${bport}")
    done
    MQTT_BROKERS_ARR=("${rewritten_mqtt[@]}")
  fi

  IFS=' ' read -r -a AMQP_BROKERS_ARR <<<"${AMQP_BROKERS}"
  declare -a resolved_amqp=()
  for tok in "${AMQP_BROKERS_ARR[@]}"; do
    if [[ "${tok}" != *:* ]]; then
      local found=""
      for def in ${DEFAULT_AMQP_BROKERS}; do
        IFS=: read -r dname _dhost _dport <<<"${def}"
        if [[ "${dname}" == "${tok}" ]]; then found="${def}"; break; fi
      done
      if [[ -n "${found}" ]]; then
        resolved_amqp+=("${found}")
      else
        log "WARN: AMQP broker '${tok}' has no host:port and was not found in defaults. Skipping."
      fi
    else
      resolved_amqp+=("${tok}")
    fi
  done
  AMQP_BROKERS_ARR=("${resolved_amqp[@]}")

  if [[ -n "${HOST}" ]]; then
    declare -a rewritten_amqp=()
    for tok in "${AMQP_BROKERS_ARR[@]}"; do
      IFS=: read -r bname _bhost bport <<<"${tok}"
      rewritten_amqp+=("${bname}:${HOST}:${bport}")
    done
    AMQP_BROKERS_ARR=("${rewritten_amqp[@]}")
  fi
}

extract_sub_steady_state_metrics() {
  local sub_csv="$1"
  awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
    NR == 1 { next }
    {
      ts = $1 + 0
      if (min_ts == "" || ts < min_ts) min_ts = ts
      if (max_ts == "" || ts > max_ts) max_ts = ts
      row_ts[NR] = ts
      row_recv[NR] = $3 + 0
      row_err[NR] = $4 + 0
      row_p50[NR] = $8 + 0
      row_p95[NR] = $10 + 0
      row_p99[NR] = $11 + 0
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
        print "0.00,0,0,0,0,0,0,0,0"
        exit
      }
      duration = last_ts - first_ts
      if (duration > 0) tps = (last_recv - first_recv) / duration
      else tps = 0
      printf "%.2f,%.0f,%.0f,%.0f,%.0f,%.0f,%d,%d,%d", tps, sum_p50 / count, sum_p95 / count, sum_p99 / count, last_recv - first_recv, last_err - first_err, count, first_ts, last_ts
    }
  ' "${sub_csv}"
}

extract_pub_steady_state_metrics() {
  local pub_csv="$1"
  awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
    NR == 1 { next }
    {
      ts = $1 + 0
      if (min_ts == "" || ts < min_ts) min_ts = ts
      if (max_ts == "" || ts > max_ts) max_ts = ts
      row_ts[NR] = ts
      row_sent[NR] = $2 + 0
      row_err[NR] = $4 + 0
      total_rows = NR
    }
    END {
      window_start = min_ts + ignore_start
      window_end = max_ts - ignore_end
      for (i = 2; i <= total_rows; i++) {
        ts = row_ts[i]
        if (ts >= window_start && ts <= window_end && row_sent[i] > 0) {
          if (first_ts == "") {
            first_ts = ts
            first_sent = row_sent[i]
            first_err = row_err[i]
          }
          last_ts = ts
          last_sent = row_sent[i]
          last_err = row_err[i]
        }
      }
      duration = last_ts - first_ts
      if (duration > 0) tps = (last_sent - first_sent) / duration
      else tps = 0
      printf "%.2f,%.0f,%.0f", tps, last_sent - first_sent, last_err - first_err
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

build_sub_cmd() {
  local -n _out="$1"; shift
  local expr="$1"; shift
  local subscribers="$1"; shift
  local csv="$1"; shift
  local args=()
  make_connect_args sub args
  _out=(
    "${BIN}" --snapshot-interval "${SNAPSHOT}" sub
    "${args[@]}"
    --expr "${expr}"
    --subscribers "${subscribers}"
    --qos "${QOS}"
    --csv "${csv}"
  )
}

build_pub_cmd() {
  local -n _out="$1"; shift
  local topic="$1"; shift
  local publishers="$1"; shift
  local payload="$1"; shift
  local rate_per_pub="$1"; shift
  local duration="$1"; shift
  local csv="$1"; shift
  local args=()
  make_connect_args pub args
  _out=(
    "${BIN}" --snapshot-interval "${SNAPSHOT}" pub
    "${args[@]}"
    --topic-prefix "${topic}"
    --topics 1
    --publishers "${publishers}"
    --payload "${payload}"
    --rate "${rate_per_pub}"
    --duration "${duration}"
    --qos "${QOS}"
    --csv "${csv}"
  )
}

print_cmd_line() {
  local out
  out=$(printf '%q ' "$@")
  echo "[cmd] ${out}"
}

print_fanin_status() {
  local sub_file="$1"
  local pub_file="$2"
  local publishers="$3"
  local last_sub last_pub
  last_sub=$(tail -n +2 "${sub_file}" 2>/dev/null | tail -n1 || true)
  last_pub=$(tail -n +2 "${pub_file}" 2>/dev/null | tail -n1 || true)

  local sent="-" pub_itps="-" pub_tps="-" pub_err="-"
  local recv="-" sub_itps="-" sub_tps="-" p50="0" p95="0" p99="0" sub_err="-"

  if [[ -n "${last_pub}" ]]; then
    IFS=, read -r _ sent _ pub_err pub_tps pub_itps _ <<<"${last_pub}"
  fi
  if [[ -n "${last_sub}" ]]; then
    IFS=, read -r _ _ recv sub_err sub_tps sub_itps _ p50 _ p95 p99 _ <<<"${last_sub}"
  fi

  printf "[status] PUB sent=%s itps=%s tps=%s pubs=%s err=%s | SUB recv=%s itps=%s tps=%s p50=%.2fms p95=%.2fms p99=%.2fms err=%s
"     "${sent}" "${pub_itps}" "${pub_tps}" "${publishers}" "${pub_err}"     "${recv}" "${sub_itps}" "${sub_tps}"     "$(awk -v n="${p50:-0}" 'BEGIN{printf (n/1e6)}')"     "$(awk -v n="${p95:-0}" 'BEGIN{printf (n/1e6)}')"     "$(awk -v n="${p99:-0}" 'BEGIN{printf (n/1e6)}')"     "${sub_err}"
}

run_workload() {
  local run_id="$1"
  local publishers="$2"
  local duration="$3"
  local payload="$4"
  local rate_per_pub="$5"
  local art_dir="$6"
  local capture_stats="$7"

  if [[ "${DRY_RUN}" != 1 ]]; then
    mkdir -p "${art_dir}"
  fi
  local sub_csv="${art_dir}/sub_agg.csv"
  local pub_csv="${art_dir}/pub_agg.csv"
  local sub_log="${art_dir}/sub.log"
  local pub_log="${art_dir}/pub.log"
  local stats_csv="${art_dir}/docker_stats.csv"
  local stats_pid=0
  local sub_pid=0
  local pub_pid=0
  local stats_duration=$(( SUB_STARTUP_DELAY + duration + 15 ))

  if [[ "${DRY_RUN}" != 1 ]]; then
    build_release_if_stale "${BIN}"
  fi

  if [[ "${DRY_RUN}" != 1 ]] && [[ "${capture_stats}" == "1" ]] && ! is_remote_host "${SUMMARY_HOST}" && [[ -n "${STATS_CONTAINER}" ]]; then
    log "Capturing docker stats for: ${STATS_CONTAINER} -> ${stats_csv}"
    start_broker_stats_monitor stats_pid "${stats_csv}" "${STATS_CONTAINER}"
  fi

  if [[ "${DRY_RUN}" != 1 ]] && [[ "${capture_stats}" == "1" ]] && is_remote_host "${SUMMARY_HOST}"; then
    local remote_stats_target
    remote_stats_target="$(get_remote_stats_target "${SUMMARY_HOST}")"
    log "Starting remote stats collector on ${remote_stats_target} for ${stats_duration}s..."
    if [[ -n "${STATS_CONTAINER}" ]]; then
      "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${remote_stats_target}" "${stats_csv}" "${stats_duration}" --extended --containers "${STATS_CONTAINER}" >/dev/null 2>&1 &
    else
      "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${remote_stats_target}" "${stats_csv}" "${stats_duration}" --extended >/dev/null 2>&1 &
    fi
    stats_pid=$!
  fi

  local sub_cmd=()
  build_sub_cmd sub_cmd "${KEY}" "${SUBSCRIBERS}" "${sub_csv}"
  log "Starting ${SUBSCRIBERS} subscriber(s) for run ${run_id}"
  print_cmd_line "${sub_cmd[@]}" && echo "       1>$(printf %q "${sub_log}") 2>&1 &"
  if [[ "${DRY_RUN}" != 1 ]]; then
    "${sub_cmd[@]}" >"${sub_log}" 2>&1 &
    sub_pid=$!
    sleep "${SUB_STARTUP_DELAY}"
  fi

  local pub_cmd=()
  build_pub_cmd pub_cmd "${KEY}" "${publishers}" "${payload}" "${rate_per_pub}" "${duration}" "${pub_csv}"
  log "Running fan-in publishers=${publishers} subscriber=${SUBSCRIBERS} total_rate=$(( publishers * rate_per_pub ))/s duration=${duration}s"
  print_cmd_line "${pub_cmd[@]}" && echo "       1>$(printf %q "${pub_log}") 2>&1"
  if [[ "${DRY_RUN}" != 1 ]]; then
    "${pub_cmd[@]}" >"${pub_log}" 2>&1 &
    pub_pid=$!

    log "[watch] printing status every ${SNAPSHOT}s..."
    while kill -0 "${pub_pid}" 2>/dev/null; do
      print_fanin_status "${sub_csv}" "${pub_csv}" "${publishers}"
      sleep "${SNAPSHOT}"
    done
    wait "${pub_pid}" || true
    print_fanin_status "${sub_csv}" "${pub_csv}" "${publishers}"

    sleep 2
    if (( sub_pid > 0 )); then
      kill -INT "${sub_pid}" >/dev/null 2>&1 || true
      wait "${sub_pid}" 2>/dev/null || true
    fi
  fi

  if (( stats_pid > 0 )); then
    stop_broker_stats_monitor "${stats_pid}"
    wait "${stats_pid}" 2>/dev/null || true
  fi
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
    log "WARN: Missing subscriber CSV in ${art_dir}"
    return 0
  fi
  if [[ ! -f "${pub_csv}" ]]; then
    log "WARN: Missing publisher CSV in ${art_dir}"
    return 0
  fi

  local sub_metrics
  sub_metrics="$(extract_sub_steady_state_metrics "${sub_csv}")"
  local sub_tps avg_p50_ns avg_p95_ns avg_p99_ns recv sub_errors steady_rows steady_start_ts steady_end_ts
  IFS=, read -r sub_tps avg_p50_ns avg_p95_ns avg_p99_ns recv sub_errors steady_rows steady_start_ts steady_end_ts <<<"${sub_metrics}"
  if [[ "${steady_rows:-0}" -eq 0 ]]; then
    log "WARN: No steady-state subscriber rows found for ${run_id}"
    return 0
  fi

  local pub_metrics pub_tps sent pub_errors
  pub_metrics="$(extract_pub_steady_state_metrics "${pub_csv}")"
  IFS=, read -r pub_tps sent pub_errors <<<"${pub_metrics}"

  local expected_recv=$(( sent * subs ))
  local loss_pct
  loss_pct=$(awk -v s="${expected_recv}" -v r="${recv}" 'BEGIN{if(s>0){printf("%.2f", (s-r)/s*100)}else{print "0.00"}}')

  local p50_ms p95_ms p99_ms
  p50_ms=$(awk -v n="${avg_p50_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')
  p95_ms=$(awk -v n="${avg_p95_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')
  p99_ms=$(awk -v n="${avg_p99_ns}" 'BEGIN{if(n==""||n==0||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')

  local errors=$(( sub_errors + pub_errors ))
  local stats_csv="${art_dir}/docker_stats.csv"
  local max_cpu="" max_mem_perc="" max_mem_used="" avg_cpu="" avg_mem_perc="" avg_mem_used=""
  local max_net_rx_bps="" max_net_tx_bps="" avg_net_rx_bps="" avg_net_tx_bps=""
  if [[ -f "${stats_csv}" ]]; then
    local agg stats_rows net_rows
    agg="$(extract_stats_metrics "${stats_csv}" "${steady_start_ts}" "${steady_end_ts}")"
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

  echo "${transport},${host},${port},${payload},${subs},${pubs},${rate_per_pub},${total_rate},${delivery_rate},${run_id},${sub_tps},${p50_ms},${p95_ms},${p99_ms},${pub_tps},${sent},${recv},${errors},${loss_pct},${art_dir},${max_cpu},${max_mem_perc},${max_mem_used},${avg_cpu},${avg_mem_perc},${avg_mem_used},${max_net_rx_bps},${max_net_tx_bps},${avg_net_rx_bps},${avg_net_tx_bps}" >> "${SUMMARY_CSV}"

  log "Summary: transport=${transport} pubs=${pubs} subs=${subs} sub_tps=${sub_tps} pub_tps=${pub_tps} recv=${recv} sent=${sent} errors=${errors} loss=${loss_pct}% p50=${p50_ms}ms p95=${p95_ms}ms p99=${p99_ms}ms"
}

SUMMARY_HOST=""
STATS_CONTAINER=""

run_single_execution() {
  local transport="$1"
  local pubs="$2"
  local payload="$3"
  local rate_per_pub="$4"
  local broker_name="${5:-}"
  local broker_host="${6:-}"
  local broker_port="${7:-}"

  local total_rate=$(( pubs * rate_per_pub ))
  local delivery_rate=$(( total_rate * SUBSCRIBERS ))
  local rid_suffix="${transport}_p${payload}_s${SUBSCRIBERS}_u${pubs}_r${rate_per_pub}"
  if [[ -n "${broker_name}" ]]; then
    rid_suffix="${broker_name}_p${payload}_s${SUBSCRIBERS}_u${pubs}_r${rate_per_pub}"
  fi

  local rid="${RUN_ID_PREFIX}_$(timestamp)_${rid_suffix}"
  local art_dir="${REPO_ROOT}/artifacts/${rid}/fanin_singlesite"
  local summary_transport="${transport}"
  local summary_host="${broker_host:-${HOST:-127.0.0.1}}"
  local summary_port="${broker_port:-}"
  local host_env_desc=""

  if [[ -n "${broker_name}" ]]; then
    if [[ "${transport}" == "mqtt" ]]; then
      summary_transport="mqtt_${broker_name}"
      STATS_CONTAINER="$(get_services "${broker_name}")"
    elif [[ "${transport}" == "amqp" ]]; then
      summary_transport="amqp_${broker_name}"
      STATS_CONTAINER="$(get_services "${broker_name}-amqp")"
    fi
  else
    STATS_CONTAINER="$(get_services "${transport}")"
  fi

  if [[ -z "${summary_port}" ]]; then
    if [[ "${transport}" == "amqp" ]] && [[ -n "${broker_name}" ]]; then
      summary_port="$(get_standard_port "${broker_name}-amqp")"
    else
      summary_port="$(get_standard_port "${transport}")"
    fi
  fi
  SUMMARY_HOST="${summary_host}"

  case "${transport}" in
    zenoh)
      ENGINE=zenoh
      if [[ -n "${HOST}" ]]; then
        ENDPOINT_SUB="tcp/${HOST}:7447"
        ENDPOINT_PUB="tcp/${HOST}:7447"
        host_env_desc="ENDPOINT_SUB=${ENDPOINT_SUB} ENDPOINT_PUB=${ENDPOINT_PUB}"
      fi
      ;;
    redis)
      ENGINE=redis
      if [[ -n "${HOST}" ]]; then REDIS_URL="redis://${HOST}:6379"; host_env_desc="REDIS_URL=${REDIS_URL}"; fi
      ;;
    nats)
      ENGINE=nats
      if [[ -n "${HOST}" ]]; then NATS_HOST="${HOST}"; host_env_desc="NATS_HOST=${NATS_HOST}"; fi
      ;;
    rabbitmq)
      ENGINE=rabbitmq
      if [[ -n "${HOST}" ]]; then RABBITMQ_HOST="${HOST}"; host_env_desc="RABBITMQ_HOST=${RABBITMQ_HOST}"; fi
      ;;
    mqtt)
      if [[ -z "${broker_name}" ]]; then log "ERROR: MQTT transport requires broker details"; return 1; fi
      ENGINE=mqtt
      MQTT_HOST="${broker_host}"
      MQTT_PORT="${broker_port}"
      MQTT_QOS="${QOS}"
      host_env_desc="MQTT_HOST=${MQTT_HOST} MQTT_PORT=${MQTT_PORT}"
      if [[ "${broker_name}" == "artemis" ]]; then
        MQTT_USERNAME="${MQTT_USERNAME:-admin}"
        MQTT_PASSWORD="${MQTT_PASSWORD:-admin}"
        host_env_desc="${host_env_desc} MQTT_USERNAME=${MQTT_USERNAME}"
      fi
      ;;
    amqp)
      if [[ -z "${broker_name}" ]]; then log "ERROR: AMQP transport requires broker details"; return 1; fi
      ENGINE=rabbitmq
      local amqp_user="guest" amqp_pass="guest" amqp_vhost="%2f"
      if [[ "${broker_name}" == "artemis" ]]; then
        amqp_user="admin"; amqp_pass="admin"; amqp_vhost=""
      fi
      RABBITMQ_URL="amqp://${amqp_user}:${amqp_pass}@${broker_host}:${broker_port}/${amqp_vhost}"
      host_env_desc="RABBITMQ_URL=${RABBITMQ_URL}"
      ;;
    *)
      log "Unknown transport: ${transport}"
      return 1
      ;;
  esac

  if [[ -n "${host_env_desc}" ]]; then log "Connection env: ${host_env_desc}"; fi

  if [[ ${WARMUP_SECS} -gt 0 ]]; then
    local warm_rid="warmup_${rid}"
    local warm_dir="${REPO_ROOT}/artifacts/${warm_rid}/fanin_singlesite"
    local warm_payload="${WARMUP_PAYLOAD:-${payload}}"
    log "Warmup: transport=${summary_transport} pubs=${pubs} duration=${WARMUP_SECS}s payload=${warm_payload}B"
    run_workload "${warm_rid}" "${pubs}" "${WARMUP_SECS}" "${warm_payload}" "${rate_per_pub}" "${warm_dir}" 0
    if [[ "${DRY_RUN}" != 1 ]]; then rm -rf "${REPO_ROOT}/artifacts/${warm_rid}" 2>/dev/null || true; fi
  fi

  log "Run: transport=${summary_transport} pubs=${pubs} subs=${SUBSCRIBERS} payload=${payload}B publish_rate=${total_rate}/s delivery_rate=${delivery_rate}/s"
  run_workload "${rid}" "${pubs}" "${DURATION}" "${payload}" "${rate_per_pub}" "${art_dir}" 1

  if [[ "${DRY_RUN}" != 1 ]]; then
    append_summary_from_artifacts "${summary_transport}" "${summary_host}" "${summary_port}" "${payload}" "${SUBSCRIBERS}" "${pubs}" "${rate_per_pub}" "${total_rate}" "${delivery_rate}" "${rid}" "${art_dir}"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --publishers-list|--pubs-list)
      shift
      IFS=' ' read -r -a PUBLISHERS_LIST <<<"${1:-}"
      ;;
    --subscribers|--subs)
      shift
      SUBSCRIBERS=${1:-1}
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
      DURATION=${1:-120}
      ;;
    --warmup)
      shift
      WARMUP_SECS=${1:-60}
      ;;
    --warmup-payload)
      shift
      WARMUP_PAYLOAD="$(to_bytes "${1:-1024}")"
      ;;
    --snapshot)
      shift
      SNAPSHOT=${1:-1}
      ;;
    --qos)
      shift
      QOS=${1:-0}
      ;;
    --key)
      shift
      KEY=${1:-bench/topic}
      ;;
    --sub-startup-delay)
      shift
      SUB_STARTUP_DELAY=${1:-5}
      ;;
    --ignore-start-secs)
      shift
      IGNORE_START_SECS=${1:-5}
      ;;
    --ignore-end-secs)
      shift
      IGNORE_END_SECS=${1:-5}
      ;;
    --transports)
      shift
      if [[ -n "${1:-}" ]]; then IFS=' ' read -r -a TRANSPORTS <<<"${1}"; fi
      ;;
    --run-id-prefix)
      shift
      RUN_ID_PREFIX=${1:-fanin_steady}
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
      MQTT_BROKERS="$(clean_quotes "${1:-}")"
      ;;
    --amqp-brokers)
      shift
      AMQP_BROKERS="$(clean_quotes "${1:-}")"
      ;;
    --interval-sec)
      shift
      INTERVAL_SEC=${1:-0}
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
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
  shift || true
done

PAYLOAD_BYTES="$(to_bytes "${PAYLOAD_TOKEN}")"
if [[ ! "${PAYLOAD_BYTES}" =~ ^[0-9]+$ ]]; then
  echo "[error] Invalid --payload: ${PAYLOAD_TOKEN}" >&2
  exit 2
fi
if [[ ! "${SUBSCRIBERS}" =~ ^[0-9]+$ ]] || (( SUBSCRIBERS <= 0 )); then
  echo "[error] Invalid --subscribers: ${SUBSCRIBERS}" >&2
  exit 2
fi
if [[ ! "${RATE_PER_PUB}" =~ ^[0-9]+$ ]] || (( RATE_PER_PUB <= 0 )); then
  echo "[error] Invalid --rate-per-pub: ${RATE_PER_PUB}" >&2
  exit 2
fi
if [[ ! "${QOS}" =~ ^[0-9]+$ ]]; then
  echo "[error] Invalid --qos: ${QOS}" >&2
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

main() {
  init_dirs
  write_summary_header
  resolve_broker_lists
  if [[ ${SEQUENTIAL} -eq 0 ]]; then
    ensure_services
  fi

  local ulim
  ulim=$(ulimit -n)
  if [[ "${ulim}" != "unlimited" ]] && (( ulim < 1048576 )); then
    log "Current ulimit -n is ${ulim}. Attempting to raise to 1048576..."
    if ulimit -n 1048576 2>/dev/null; then
      log "Success: ulimit -n is now $(ulimit -n)"
    elif ulimit -n 65536 2>/dev/null; then
      log "Success: ulimit -n is now $(ulimit -n)"
    else
      log "WARN: Failed to raise ulimit. Large fan-in runs may hit file descriptor limits."
    fi
  fi

  log "Fan-in steady-load benchmark -> ${BENCH_DIR}"
  log "Resolved: SUMMARY_CSV=${SUMMARY_CSV} | PLOTS_DIR=${PLOTS_DIR} | RAW_DIR=${RAW_DIR}"
  log "Resolved payload (bytes): ${PAYLOAD_BYTES}"
  log "Publisher sweep: ${PUBLISHERS_LIST[*]}"
  log "Subscribers=${SUBSCRIBERS} | rate_per_pub=${RATE_PER_PUB}/s | duration=${DURATION}s | warmup=${WARMUP_SECS}s | qos=${QOS}"

  for t in "${TRANSPORTS[@]}"; do
    if [[ "${t}" == "mqtt" ]]; then
      if [[ ${#MQTT_BROKERS_ARR[@]} -eq 0 ]]; then
        log "WARN: No MQTT brokers defined; skipping"
        continue
      fi
      for b in "${MQTT_BROKERS_ARR[@]}"; do
        IFS=: read -r bname bhost bport <<<"${b}"
        local svc=""
        local first_iteration=1
        if [[ ${SEQUENTIAL} -eq 1 ]]; then svc="$(get_services "${bname}")"; fi
        for p in "${PUBLISHERS_LIST[@]}"; do
          if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
            if [[ ${first_iteration} -eq 1 ]]; then
              manage_service up "${svc}"
              first_iteration=0
            else
              manage_service restart "${svc}"
            fi
            wait_for_port "${bhost}" "${bport}"
          fi
          log "Queue: transport=mqtt broker=${bname} publishers=${p}"
          run_single_execution "mqtt" "${p}" "${PAYLOAD_BYTES}" "${RATE_PER_PUB}" "${bname}" "${bhost}" "${bport}"
          if [[ ${INTERVAL_SEC} -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
            log "Sleeping ${INTERVAL_SEC}s between runs..."
            sleep "${INTERVAL_SEC}"
          fi
        done
        if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
          manage_service down "${svc}"
        fi
      done
    elif [[ "${t}" == "amqp" ]]; then
      if [[ ${#AMQP_BROKERS_ARR[@]} -eq 0 ]]; then
        log "WARN: No AMQP brokers defined; skipping"
        continue
      fi
      for b in "${AMQP_BROKERS_ARR[@]}"; do
        IFS=: read -r bname bhost bport <<<"${b}"
        local svc=""
        local first_iteration=1
        if [[ ${SEQUENTIAL} -eq 1 ]]; then svc="$(get_services "${bname}-amqp")"; fi
        for p in "${PUBLISHERS_LIST[@]}"; do
          if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
            if [[ ${first_iteration} -eq 1 ]]; then
              manage_service up "${svc}"
              first_iteration=0
            else
              manage_service restart "${svc}"
            fi
            wait_for_port "${bhost}" "${bport}"
          fi
          log "Queue: transport=amqp broker=${bname} publishers=${p}"
          run_single_execution "amqp" "${p}" "${PAYLOAD_BYTES}" "${RATE_PER_PUB}" "${bname}" "${bhost}" "${bport}"
          if [[ ${INTERVAL_SEC} -gt 0 ]] && [[ "${DRY_RUN}" != 1 ]]; then
            log "Sleeping ${INTERVAL_SEC}s between runs..."
            sleep "${INTERVAL_SEC}"
          fi
        done
        if [[ ${SEQUENTIAL} -eq 1 ]] && [[ -n "${svc}" ]]; then
          manage_service down "${svc}"
        fi
      done
    else
      local svc=""
      local port=""
      local first_iteration=1
      if [[ ${SEQUENTIAL} -eq 1 ]]; then
        svc="$(get_services "${t}")"
        port="$(get_standard_port "${t}")"
      fi
      for p in "${PUBLISHERS_LIST[@]}"; do
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
        fi
        log "Queue: transport=${t} publishers=${p}"
        run_single_execution "${t}" "${p}" "${PAYLOAD_BYTES}" "${RATE_PER_PUB}"
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

  local latex_plots_dir="${PLOTS_DIR}/latex"
  log "Plotting results to ${PLOTS_DIR}"
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ python3 ${SCRIPT_DIR}/plot_results.py --summary ${SUMMARY_CSV} --out-dir ${PLOTS_DIR}"
    echo "+ python3 ${SCRIPT_DIR}/plot_results.py --summary ${SUMMARY_CSV} --out-dir ${latex_plots_dir} --latex"
  else
    python3 "${SCRIPT_DIR}/plot_results.py" --summary "${SUMMARY_CSV}" --out-dir "${PLOTS_DIR}"
    log "Plotting LaTeX-ready PDF results to ${latex_plots_dir}"
    python3 "${SCRIPT_DIR}/plot_results.py" --summary "${SUMMARY_CSV}" --out-dir "${latex_plots_dir}" --latex
  fi

  log "Done. Summary CSV: ${SUMMARY_CSV}"
  log "Plots: ${PLOTS_DIR} | LaTeX plots: ${latex_plots_dir}"
}

main "$@"
