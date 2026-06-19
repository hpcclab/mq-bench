#!/usr/bin/env bash
set -euo pipefail

# Orchestrate Experiment 3: Bursty Fan-Out Under Dynamic Traffic.
#
# Topology:
#   Controlled fan-out: P independent publisher/topic groups.
#   Each publisher publishes to its own topic, and each topic has SUBS_PER_PUB
#   subscribers. Use --sub-procs-per-topic to split each topic's subscribers
#   across multiple client processes when the subscriber harness is the bottleneck.
#   This differs from broadcast fan-out, where one shared topic
#   causes every subscriber to receive every published message.
#
# Default traffic profile, per publisher:
#   warmup:60:10,baseline:60:10,burst:60:30,elevated:60:15,recovery:60:10
#
# Output:
#   results/fanout_bursty_load_<ts>/{raw_data,plots}/
#   raw_data/summary_by_phase.csv contains one row per transport/broker/phase.
#
# Usage examples:
#   scripts/orchestrate_fanout_bursty_load.sh
#   scripts/orchestrate_fanout_bursty_load.sh --host 192.168.0.245 --transports "zenoh nats"
#   scripts/orchestrate_fanout_bursty_load.sh --transports "mqtt" --mqtt-brokers "mosquitto emqx artemis"
#   scripts/orchestrate_fanout_bursty_load.sh --dry-run --transports "zenoh"
#   scripts/orchestrate_fanout_bursty_load.sh --append-to results/fanout_bursty_load_YYYYmmdd_HHMMSS --transports "nats" --rate-profile "burst:60:8000,recovery:60:100"
#   scripts/orchestrate_fanout_bursty_load.sh --append-after-current --transports "nats" --rate-profile "burst:60:8000,recovery:60:100"
#   scripts/orchestrate_fanout_bursty_load.sh --min-delivery-ratio 0.70 --continue-on-broker-fail
#   scripts/orchestrate_fanout_bursty_load.sh --abort-on-under-target

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/lib.sh"

SUBS=1000
SUBS_PER_PUB=100
SUB_PROCS_PER_TOPIC=${SUB_PROCS_PER_TOPIC:-1}
PUBLISHERS=""
NUM_PUBS=0
PAYLOAD_TOKEN="1024"
SNAPSHOT=1
QOS=0
KEY="bench/topic"
RUN_ID_PREFIX="fanout_bursty"
RATE_PROFILE="warmup:60:10,baseline:60:10,burst:60:30,elevated:60:15,recovery:60:10"
PLOT_BUCKET_SECONDS="${PLOT_BUCKET_SECONDS:-1}"
TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
DEFAULT_TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
START_SERVICES=0
DRY_RUN=${DRY_RUN:-0}
HOST="${HOST:-}"
INTERVAL_SEC=${INTERVAL_SEC:-120}
SEQUENTIAL=0
SSH_TARGET=""
REMOTE_DIR="~/mq-bench"
APPEND_LATEST=0
APPEND_TO_DIR=""
WAIT_FOR_CURRENT_APPEND=0
PHASE_OFFSET_SECONDS=0
SUMMARY_OVERRIDE="${SUMMARY_OVERRIDE:-}"
SUB_RAMP_UP_SECS="${SUB_RAMP_UP_SECS:-}"
ABORT_ON_UNDER_TARGET="${ABORT_ON_UNDER_TARGET:-0}"
MIN_DELIVERY_RATIO="${MIN_DELIVERY_RATIO:-0.70}"
PHASE_GRACE_SECS="${PHASE_GRACE_SECS:-10}"
STUCK_TIMEOUT_SECS="${STUCK_TIMEOUT_SECS:-30}"
CONTINUE_ON_BROKER_FAIL=0

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
DURATION=0
RUNS_REMAINING=0
RUN_FAILURES=0
STOP_AFTER_CURRENT=0

log() { echo "[$(date +%H:%M:%S)] $*"; }
run() { if [[ "${DRY_RUN}" = 1 ]]; then echo "+ $*"; else eval "$*"; fi; }
timestamp() { date +%Y%m%d_%H%M%S; }

usage() {
  sed -n '1,80p' "$0" | sed -n 's/^# //p'
}

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

profile_duration_secs() {
  local profile="$1"
  awk -v spec="${profile}" '
    BEGIN {
      n = split(spec, phases, ",")
      if (n < 1) exit 1
      for (i = 1; i <= n; i++) {
        m = split(phases[i], parts, ":")
        if (m != 3 || parts[2] !~ /^[0-9]+$/ || parts[2] <= 0) exit 1
        total += parts[2]
      }
      print total
    }
  '
}

current_orchestrator_pids() {
  if ! command -v pgrep >/dev/null 2>&1; then return 0; fi
  local pid
  pgrep -f "scripts/orchestrate_fanout_bursty_load.sh" 2>/dev/null | while read -r pid; do
    if [[ -z "${pid}" || "${pid}" == "$$" || "${pid}" == "${BASHPID}" ]]; then
      continue
    fi
    if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
      echo "${pid}"
    fi
  done
}

wait_for_current_orchestrators() {
  if [[ "${WAIT_FOR_CURRENT_APPEND}" -ne 1 ]]; then return 0; fi
  APPEND_LATEST=1
  while true; do
    local pids
    pids="$(current_orchestrator_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
    if [[ -z "${pids}" ]]; then
      log "No active benchmark orchestrator found; proceeding with append-latest run"
      return 0
    fi
    log "Waiting for active benchmark orchestrator(s) to finish before appending: ${pids}"
    sleep 15
  done
}

summary_max_phase_end() {
  local summary="${1:-}"
  if [[ -z "${summary}" || ! -s "${summary}" ]]; then echo 0; return 0; fi
  python3 - "${summary}" <<'PY'
import csv
import sys

max_end = 0.0
with open(sys.argv[1], newline="") as fh:
    for row in csv.DictReader(fh):
        try:
            max_end = max(max_end, float(row.get("phase_end_s") or 0))
        except ValueError:
            pass
print(int(max_end) if max_end.is_integer() else max_end)
PY
}

write_phase_rate_summary() {
  local out="${BENCH_DIR}/phase-rate-summary.md"
  if [[ "${PHASE_OFFSET_SECONDS:-0}" != "0" && "${PHASE_OFFSET_SECONDS:-0}" != "0.0" ]]; then
    out="${BENCH_DIR}/phase-rate-summary-append-$(timestamp).md"
  fi
  local pubs subs subs_per_pub payload profile
  pubs="$(calc_pubs_for_subs)"
  subs="${SUBS}"
  subs_per_pub="${SUBS_PER_PUB}"
  payload="${PAYLOAD_BYTES}"
  profile="${RATE_PROFILE}"

  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ write ${out}"
    return 0
  fi

  python3 - "${out}" "${profile}" "${subs}" "${pubs}" "${subs_per_pub}" "${payload}" <<'PY'
import sys

out, profile, subs_s, pubs_s, subs_per_pub_s, payload_s = sys.argv[1:]
subs = int(subs_s)
pubs = int(pubs_s)
subs_per_pub = int(subs_per_pub_s)
payload = int(payload_s)

def fmt_rate(value):
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.0f}k"
    return f"{value:.0f}"

rows = []
for item in profile.split(","):
    parts = item.split(":")
    if len(parts) != 3:
        continue
    phase, duration_s, rate_s = parts
    rate_per_pub = float(rate_s)
    total_pub_rate = rate_per_pub * pubs
    fanout_target = total_pub_rate * subs_per_pub
    rows.append((phase, duration_s, rate_per_pub, total_pub_rate, fanout_target))

with open(out, "w", encoding="utf-8") as fh:
    fh.write("# Fan-Out Bursty Load Phase Rates\n\n")
    fh.write("Source profile: `" + profile + "`\n\n")
    fh.write("## Run Setup\n\n")
    fh.write(f"- Subscribers: `{subs}`\n")
    fh.write(f"- Publishers: `{pubs}`\n")
    fh.write(f"- Subscribers per publisher/topic: `{subs_per_pub}`\n")
    fh.write(f"- Payload: `{payload} bytes`\n\n")
    fh.write("## Rate Definitions\n\n")
    fh.write("```text\n")
    fh.write("total publish rate = rate_per_publisher * publishers\n")
    fh.write("controlled fan-out delivery target = total publish rate * subscribers_per_publisher\n")
    fh.write("```\n\n")
    fh.write("## Phase Message Rates\n\n")
    fh.write("| Phase | Duration | Rate per publisher | Total publish rate | Fan-out delivery target |\n")
    fh.write("|---|---:|---:|---:|---:|\n")
    for phase, duration_s, rate_per_pub, total_pub_rate, fanout_target in rows:
        fh.write(
            f"| {phase} | {duration_s}s | {fmt_rate(rate_per_pub)} msg/s | "
            f"{fmt_rate(total_pub_rate)} msg/s | {fmt_rate(fanout_target)} msg/s |\n"
        )
PY
  log "Wrote phase-rate summary: ${out}"
}

calc_pubs_for_subs() {
  if (( NUM_PUBS > 0 )); then
    echo "${NUM_PUBS}"
    return 0
  fi
  if [[ "${SUBS}" =~ ^[0-9]+$ && "${SUBS_PER_PUB}" =~ ^[0-9]+$ ]] && (( SUBS_PER_PUB > 0 )); then
    echo $(( SUBS / SUBS_PER_PUB ))
  else
    echo 0
  fi
}

validate_controlled_fanout() {
  if [[ ! "${SUBS}" =~ ^[0-9]+$ ]] || (( SUBS <= 0 )); then
    echo "[error] Invalid --subs: ${SUBS}" >&2
    exit 2
  fi
  if [[ ! "${SUBS_PER_PUB}" =~ ^[0-9]+$ ]] || (( SUBS_PER_PUB <= 0 )); then
    echo "[error] Invalid --subs-per-pub: ${SUBS_PER_PUB}" >&2
    exit 2
  fi
  if (( SUBS % SUBS_PER_PUB != 0 )); then
    echo "ERROR: --subs must be divisible by --subs-per-pub for controlled fan-out" >&2
    exit 1
  fi

  NUM_PUBS=$(( SUBS / SUBS_PER_PUB ))
  if [[ -n "${PUBLISHERS}" ]]; then
    if [[ ! "${PUBLISHERS}" =~ ^[0-9]+$ ]] || (( PUBLISHERS <= 0 )); then
      echo "[error] Invalid --publishers: ${PUBLISHERS}" >&2
      exit 2
    fi
    if (( PUBLISHERS != NUM_PUBS )); then
      echo "[error] --publishers is derived from --subs / --subs-per-pub for controlled fan-out; expected ${NUM_PUBS}, got ${PUBLISHERS}" >&2
      exit 2
    fi
  fi
}

print_controlled_fanout_summary() {
  echo "Controlled fan-out configuration:"
  echo "Total subscribers: ${SUBS}"
  echo "Subscribers per publisher/topic: ${SUBS_PER_PUB}"
  echo "Publishers/topics: ${NUM_PUBS}"
  echo "Effective fan-out ratio: 1:${SUBS_PER_PUB}"
  echo "Expected delivery target per phase:"
  python3 - "${RATE_PROFILE}" "${NUM_PUBS}" "${SUBS_PER_PUB}" <<'PY'
import sys

profile, pubs_s, subs_per_pub_s = sys.argv[1:]
pubs = int(pubs_s)
subs_per_pub = int(subs_per_pub_s)

def fmt(value):
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value)):,}"
    return f"{value:,.2f}"

for item in profile.split(','):
    parts = item.split(':')
    if len(parts) != 3:
        continue
    phase, duration_s, rate_s = parts
    rate = float(rate_s)
    target = pubs * rate * subs_per_pub
    print(f"  {phase}:{duration_s}s rate {fmt(rate)} msg/s/pub -> {fmt(target)} delivered msg/s")
PY
}

calc_sub_ramp_up_secs() {
  if [[ -n "${SUB_RAMP_UP_SECS}" ]]; then echo "${SUB_RAMP_UP_SECS}"; return 0; fi
  if (( SUBS >= 2000 )); then echo 60; else echo 0; fi
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
    rabbitmq|rabbitmq-amqp) echo "5672" ;;
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
    if [[ "${DRY_RUN}" = 1 ]]; then echo "+ ssh ${SSH_TARGET} \"${rcmd}\""; else ssh -o BatchMode=yes "${SSH_TARGET}" "${rcmd}"; fi
  else
    log "[local] ${cmd}"
    run "${cmd}"
  fi
}

wait_for_port() {
  local host="$1" port="$2" timeout="${3:-60}"
  if [[ "${DRY_RUN}" = 1 ]]; then log "[dry-run] Would wait for ${host}:${port}"; return 0; fi
  log "Waiting for ${host}:${port}..."
  local start_ts now_ts
  start_ts=$(date +%s)
  while true; do
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; then
      log "Port ${host}:${port} is open."
      return 0
    fi
    now_ts=$(date +%s)
    if (( now_ts - start_ts > timeout )); then log "Timeout waiting for ${host}:${port}"; return 1; fi
    sleep 1
  done
}

cleanup_processes() {
  if [[ "${DRY_RUN}" = 1 ]]; then return 0; fi
  pkill -f "mq-bench.*sub" 2>/dev/null || true
  pkill -f "mq-bench.*pub" 2>/dev/null || true
  sleep 2
}

cooldown_between_runs() {
  local label="${1:-broker runs}"
  cleanup_processes
  if [[ "${INTERVAL_SEC}" =~ ^[0-9]+$ ]] && [[ "${INTERVAL_SEC}" -gt 0 ]]; then
    log "Cooldown after ${label}: cleaning clients and waiting ${INTERVAL_SEC}s for broker/network state to settle"
    if [[ "${DRY_RUN}" = 1 ]]; then
      echo "+ sleep ${INTERVAL_SEC}"
    else
      sleep "${INTERVAL_SEC}"
    fi
  fi
}

manage_service() {
  local action="$1" services="$2"
  if [[ ${SEQUENTIAL} -eq 0 || -z "${services}" ]]; then return 0; fi
  if [[ "${action}" == "up" || "${action}" == "restart" ]]; then
    cleanup_processes
    docker_compose_cmd down
    if [[ "${DRY_RUN}" != 1 ]]; then sleep 10; fi
    docker_compose_cmd "up -d" "${services}"
    if [[ "${DRY_RUN}" != 1 ]]; then sleep 5; fi
  elif [[ "${action}" == "down" ]]; then
    docker_compose_cmd down
  fi
}

init_dirs() {
  if [[ -n "${APPEND_TO_DIR}" ]]; then
    case "${APPEND_TO_DIR}" in
      /*) BENCH_DIR="${APPEND_TO_DIR}" ;;
      *) BENCH_DIR="${REPO_ROOT}/${APPEND_TO_DIR}" ;;
    esac
    APPEND_LATEST=1
  fi
  if [[ ${APPEND_LATEST} -eq 1 ]] && [[ -z "${SUMMARY_OVERRIDE}" ]] && [[ -z "${BENCH_DIR}" ]]; then
    local latest_dir
    latest_dir=$(ls -1d "${REPO_ROOT}/results/fanout_bursty_load_"* 2>/dev/null | sort -r | head -1 || true)
    if [[ -n "${latest_dir}" && -d "${latest_dir}" ]]; then
      BENCH_DIR="${latest_dir}"
      RAW_DIR="${BENCH_DIR}/raw_data"
      PLOTS_DIR="${BENCH_DIR}/plots"
      TS="${latest_dir##*fanout_bursty_load_}"
      log "Appending to existing run: ${BENCH_DIR}"
    fi
  fi
  if [[ -z "${TS}" ]]; then TS="$(timestamp)"; fi
  if [[ -z "${BENCH_DIR}" ]]; then BENCH_DIR="${REPO_ROOT}/results/fanout_bursty_load_${TS}"; fi
  if [[ -z "${RAW_DIR}" ]]; then RAW_DIR="${BENCH_DIR}/raw_data"; fi
  if [[ -z "${PLOTS_DIR}" ]]; then PLOTS_DIR="${BENCH_DIR}/plots"; fi
  case "${RAW_DIR}" in /*) ;; *) RAW_DIR="${REPO_ROOT}/${RAW_DIR}" ;; esac
  case "${PLOTS_DIR}" in /*) ;; *) PLOTS_DIR="${REPO_ROOT}/${PLOTS_DIR}" ;; esac
  mkdir -p "${RAW_DIR}" "${PLOTS_DIR}"
  if [[ -n "${SUMMARY_OVERRIDE}" ]]; then
    case "${SUMMARY_OVERRIDE}" in
      /*) SUMMARY_CSV="${SUMMARY_OVERRIDE}" ;;
      *) SUMMARY_CSV="${REPO_ROOT}/${SUMMARY_OVERRIDE}" ;;
    esac
    mkdir -p "$(dirname -- "${SUMMARY_CSV}")"
  else
    SUMMARY_CSV="${RAW_DIR}/summary_by_phase.csv"
  fi
  if [[ ${APPEND_LATEST} -eq 1 ]]; then
    PHASE_OFFSET_SECONDS="$(summary_max_phase_end "${SUMMARY_CSV}")"
    log "Append phase offset: ${PHASE_OFFSET_SECONDS}s"
  fi
}

resolve_named_brokers() {
  local raw="$1" defaults="$2" out_name="$3"
  local -n out_ref="${out_name}"
  local toks=() resolved=()
  IFS=' ' read -r -a toks <<<"${raw}"
  for tok in "${toks[@]}"; do
    if [[ "${tok}" != *:* ]]; then
      local found=""
      for def in ${defaults}; do
        IFS=: read -r dname _dhost _dport <<<"${def}"
        if [[ "${dname}" == "${tok}" ]]; then found="${def}"; break; fi
      done
      if [[ -n "${found}" ]]; then resolved+=("${found}"); else log "WARN: Unknown broker '${tok}', skipping"; fi
    else
      resolved+=("${tok}")
    fi
  done
  out_ref=("${resolved[@]}")
}

rewrite_broker_hosts() {
  local out_name="$1"
  local -n arr="${out_name}"
  if [[ -z "${HOST}" ]]; then return 0; fi
  local rewritten=()
  for tok in "${arr[@]}"; do
    IFS=: read -r bname _bhost bport <<<"${tok}"
    rewritten+=("${bname}:${HOST}:${bport}")
  done
  arr=("${rewritten[@]}")
}

copy_run_raw_data() {
  local rid="$1" source_dir="$2" dest_dir="${RAW_DIR}/${rid}/fanout_singlesite"
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ mkdir -p ${dest_dir}" >&2
    echo "+ cp -a ${source_dir}/. ${dest_dir}/" >&2
    echo "${dest_dir}"
    return 0
  fi
  mkdir -p "${dest_dir}"
  cp -a "${source_dir}/." "${dest_dir}/"
  echo "${dest_dir}"
}

summarize_run() {
  local transport="$1" host="$2" port="$3" payload="$4" subs="$5" pubs="$6" rid="$7" art_dir="$8"
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ python3 ${SCRIPT_DIR}/summarize_bursty_fanout.py --out ${SUMMARY_CSV} --transport ${transport} --host ${host} --port ${port} --payload ${payload} --subs ${subs} --pubs ${pubs} --subs-per-pub ${SUBS_PER_PUB} --profile ${RATE_PROFILE} --phase-offset ${PHASE_OFFSET_SECONDS} --run-id ${rid} --artifacts-dir ${art_dir}"
    return 0
  fi
  python3 "${SCRIPT_DIR}/summarize_bursty_fanout.py" \
    --out "${SUMMARY_CSV}" \
    --transport "${transport}" \
    --host "${host}" \
    --port "${port}" \
    --payload "${payload}" \
    --subs "${subs}" \
    --pubs "${pubs}" \
    --subs-per-pub "${SUBS_PER_PUB}" \
    --profile "${RATE_PROFILE}" \
    --phase-offset "${PHASE_OFFSET_SECONDS}" \
    --run-id "${rid}" \
    --artifacts-dir "${art_dir}"
}

run_single_execution() {
  local transport="$1" broker_name="${2:-}" broker_host="${3:-}" broker_port="${4:-}"
  local pubs payload sub_ramp_up_secs sub_ramp_duration_secs host_env monitor_env summary_transport summary_host summary_port rid_suffix rid art_dir env_common stats_container stats_pid stats_csv stats_duration remote_stats_target run_status raw_art_dir
  cleanup_processes
  pubs="$(calc_pubs_for_subs)"
  payload="${PAYLOAD_BYTES}"
  sub_ramp_up_secs="$(calc_sub_ramp_up_secs)"
  sub_ramp_duration_secs="${sub_ramp_up_secs%%.*}"
  if [[ ! "${sub_ramp_duration_secs}" =~ ^[0-9]+$ ]]; then sub_ramp_duration_secs=0; fi
  summary_transport="${transport}"
  summary_host="${broker_host:-${HOST:-127.0.0.1}}"
  summary_port="${broker_port:-}"
  stats_container=""

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
    if [[ "${transport}" == "amqp" && -n "${broker_name}" ]]; then summary_port="$(get_standard_port "${broker_name}-amqp")"; else summary_port="$(get_standard_port "${transport}")"; fi
  fi

  rid_suffix="${summary_transport}_p${payload}_s${SUBS}_u${pubs}_bursty"
  rid="${RUN_ID_PREFIX}_$(timestamp)_${rid_suffix}"
  art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
  mkdir -p "${art_dir}"

  local profile_q
  profile_q=$(printf '%q' "${RATE_PROFILE}")
  env_common="PUBS=${pubs} SUBS=${SUBS} SUBS_PER_PUB=${SUBS_PER_PUB} SUB_PROCS_PER_TOPIC=${SUB_PROCS_PER_TOPIC} CONTROLLED_FANOUT=1 SUB_RAMP_UP_SECS=${sub_ramp_up_secs} RATE_PROFILE=${profile_q} PAYLOAD=${payload} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT} ABORT_ON_UNDER_TARGET=${ABORT_ON_UNDER_TARGET} MIN_DELIVERY_RATIO=${MIN_DELIVERY_RATIO} PHASE_GRACE_SECS=${PHASE_GRACE_SECS} STUCK_TIMEOUT_SECS=${STUCK_TIMEOUT_SECS}"
  host_env=""
  monitor_env=""
  if ! is_remote_host "${summary_host}" && [[ -n "${stats_container}" ]]; then monitor_env="MONITOR_CONTAINERS=${stats_container}"; fi

  stats_pid=0
  stats_csv="${art_dir}/docker_stats.csv"
  stats_duration=$(( DURATION + sub_ramp_duration_secs + 15 ))
  if [[ "${DRY_RUN}" != 1 ]] && is_remote_host "${summary_host}"; then
    remote_stats_target="$(get_remote_stats_target "${summary_host}")"
    log "Starting remote stats collector on ${remote_stats_target} for ${stats_duration}s..."
    if [[ -n "${stats_container}" ]]; then
      "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${remote_stats_target}" "${stats_csv}" "${stats_duration}" --extended --containers "${stats_container}" >/dev/null 2>&1 &
    else
      "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${remote_stats_target}" "${stats_csv}" "${stats_duration}" --extended >/dev/null 2>&1 &
    fi
    stats_pid=$!
  fi

  log "Run: transport=${summary_transport} subs=${SUBS} pubs=${pubs} payload=${payload}B profile=${RATE_PROFILE}"
  run_status=0
  set +e
  case "${transport}" in
    zenoh)
      if [[ -n "${HOST}" ]]; then host_env="ENDPOINT_SUB=tcp/${HOST}:7447 ENDPOINT_PUB=tcp/${HOST}:7447"; fi
      run "${monitor_env} ENGINE=zenoh ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      run_status=$?
      ;;
    redis)
      if [[ -n "${HOST}" ]]; then host_env="REDIS_URL=redis://${HOST}:6379"; fi
      run "${monitor_env} ENGINE=redis ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      run_status=$?
      ;;
    nats)
      if [[ -n "${HOST}" ]]; then host_env="NATS_HOST=${HOST}"; fi
      run "${monitor_env} ENGINE=nats ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      run_status=$?
      ;;
    rabbitmq)
      if [[ -n "${HOST}" ]]; then host_env="RABBITMQ_HOST=${HOST}"; fi
      run "${monitor_env} ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      run_status=$?
      ;;
    mqtt)
      host_env="MQTT_HOST=${broker_host} MQTT_PORT=${broker_port}"
      if [[ "${broker_name}" == "artemis" ]]; then host_env="${host_env} MQTT_USERNAME=admin MQTT_PASSWORD=admin"; fi
      run "${monitor_env} ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      run_status=$?
      ;;
    amqp)
      local amqp_user="guest" amqp_pass="guest" amqp_vhost="%2f"
      if [[ "${broker_name}" == "artemis" ]]; then amqp_user="admin"; amqp_pass="admin"; amqp_vhost=""; fi
      host_env="RABBITMQ_URL=amqp://${amqp_user}:${amqp_pass}@${broker_host}:${broker_port}/${amqp_vhost}"
      run "${monitor_env} ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      run_status=$?
      ;;
    *)
      log "Unknown transport: ${transport}"
      run_status=1
      ;;
  esac
  set -e

  if [[ -n "${stats_pid}" ]] && (( stats_pid > 0 )); then kill "${stats_pid}" 2>/dev/null || true; wait "${stats_pid}" 2>/dev/null || true; fi
  raw_art_dir="$(copy_run_raw_data "${rid}" "${art_dir}")"
  summarize_run "${summary_transport}" "${summary_host}" "${summary_port}" "${payload}" "${SUBS}" "${pubs}" "${rid}" "${raw_art_dir}" || {
    local summarize_status=$?
    if (( run_status == 0 )); then run_status=${summarize_status}; fi
  }
  return "${run_status}"
}

count_planned_runs() {
  local total=0 t
  for t in "${TRANSPORTS[@]}"; do
    if [[ "${t}" == "mqtt" ]]; then
      total=$(( total + ${#MQTT_BROKERS_ARR[@]} ))
    elif [[ "${t}" == "amqp" ]]; then
      total=$(( total + ${#AMQP_BROKERS_ARR[@]} ))
    else
      total=$(( total + 1 ))
    fi
  done
  echo "${total}"
}

cooldown_if_more_runs() {
  local label="${1:-broker runs}"
  if (( RUNS_REMAINING > 0 )); then
    RUNS_REMAINING=$(( RUNS_REMAINING - 1 ))
  fi
  if (( RUNS_REMAINING > 0 )); then
    cooldown_between_runs "${label}"
  fi
}

record_run_status() {
  local status="$1"
  if (( status == 0 )); then return 0; fi
  if (( RUN_FAILURES == 0 )); then RUN_FAILURES=${status}; fi
  if (( CONTINUE_ON_BROKER_FAIL == 0 )); then STOP_AFTER_CURRENT=1; fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subs) shift; SUBS=${1:-1000} ;;
    --subs-per-pub) shift; SUBS_PER_PUB=${1:-100} ;;
    --sub-procs-per-topic) shift; SUB_PROCS_PER_TOPIC=${1:-1} ;;
    --publishers) shift; PUBLISHERS=${1:-} ;;
    --payload) shift; PAYLOAD_TOKEN=${1:-1024} ;;
    --rate-profile) shift; RATE_PROFILE=${1:-} ;;
    --snapshot) shift; SNAPSHOT=${1:-1} ;;
    --transports) shift; if [[ -n "${1:-}" ]]; then IFS=' ' read -r -a TRANSPORTS <<<"${1}"; fi ;;
    --host) shift; HOST=${1:-} ;;
    --start-services) START_SERVICES=1 ;;
    --dry-run) DRY_RUN=1 ;;
    --summary) shift; SUMMARY_OVERRIDE=${1:-} ;;
    --out-dir) shift; PLOTS_DIR=${1:-} ;;
    --raw-dir) shift; RAW_DIR=${1:-} ;;
    --bench-dir) shift; BENCH_DIR=${1:-} ;;
    --mqtt-brokers) shift; MQTT_BROKERS="$(clean_quotes "${1:-}")" ;;
    --amqp-brokers) shift; AMQP_BROKERS="$(clean_quotes "${1:-}")" ;;
    --interval-sec|--cooldown-sec) shift; INTERVAL_SEC=${1:-0} ;;
    --sequential) SEQUENTIAL=1 ;;
    --ssh-target) shift; SSH_TARGET=${1:-} ;;
    --remote-dir) shift; REMOTE_DIR=${1:-} ;;
    --append-latest) APPEND_LATEST=1 ;;
    --append-to) shift; APPEND_TO_DIR=${1:-} ;;
    --append-after-current) WAIT_FOR_CURRENT_APPEND=1 ;;
    --plot-bucket-seconds) shift; PLOT_BUCKET_SECONDS=${1:-1} ;;
    --min-delivery-ratio) shift; MIN_DELIVERY_RATIO=${1:-0.70} ;;
    --phase-grace-secs) shift; PHASE_GRACE_SECS=${1:-10} ;;
    --stuck-timeout-secs) shift; STUCK_TIMEOUT_SECS=${1:-30} ;;
    --continue-on-broker-fail) CONTINUE_ON_BROKER_FAIL=1 ;;
    --abort-on-under-target) ABORT_ON_UNDER_TARGET=1 ;;
    --no-abort-on-under-target) ABORT_ON_UNDER_TARGET=0 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
  shift || true
done

if [[ ${#TRANSPORTS[@]} -eq 0 ]]; then TRANSPORTS=("${DEFAULT_TRANSPORTS[@]}"); fi
if [[ -z "${HOST}" && -n "${SSH_TARGET}" ]]; then
  if [[ "${SSH_TARGET}" == *"@"* ]]; then HOST="${SSH_TARGET#*@}"; else HOST="${SSH_TARGET}"; fi
  log "Inferred HOST=${HOST} from SSH_TARGET"
fi

PAYLOAD_BYTES="$(to_bytes "${PAYLOAD_TOKEN}")"
if [[ ! "${PAYLOAD_BYTES}" =~ ^[0-9]+$ ]]; then echo "[error] Invalid --payload: ${PAYLOAD_TOKEN}" >&2; exit 2; fi
if [[ -z "${RATE_PROFILE}" ]]; then echo "[error] --rate-profile cannot be empty" >&2; exit 2; fi
if ! DURATION="$(profile_duration_secs "${RATE_PROFILE}")"; then echo "[error] Invalid --rate-profile: ${RATE_PROFILE}" >&2; exit 2; fi
validate_controlled_fanout

wait_for_current_orchestrators
resolve_named_brokers "${MQTT_BROKERS}" "${DEFAULT_MQTT_BROKERS}" MQTT_BROKERS_ARR
resolve_named_brokers "${AMQP_BROKERS}" "${DEFAULT_AMQP_BROKERS}" AMQP_BROKERS_ARR
rewrite_broker_hosts MQTT_BROKERS_ARR
rewrite_broker_hosts AMQP_BROKERS_ARR

init_dirs
RUNS_REMAINING="$(count_planned_runs)"
log "Fan-out bursty-load benchmark -> ${BENCH_DIR}"
log "Resolved: SUMMARY_CSV=${SUMMARY_CSV} | PLOTS_DIR=${PLOTS_DIR} | RAW_DIR=${RAW_DIR}"
log "Resolved payload=${PAYLOAD_BYTES}B subs=${SUBS} pubs=$(calc_pubs_for_subs) duration=${DURATION}s profile=${RATE_PROFILE}"
log "Inter-run cooldown: ${INTERVAL_SEC}s (use --cooldown-sec N or --interval-sec N to override)"
print_controlled_fanout_summary

if [[ ${START_SERVICES} -eq 1 && ${SEQUENTIAL} -eq 0 ]]; then
  if [[ -n "${HOST}" && "${HOST}" != "127.0.0.1" && "${HOST}" != "localhost" ]]; then
    log "HOST=${HOST} indicates remote brokers; skipping local service startup"
  else
    run "bash \"${SCRIPT_DIR}/compose_up.sh\""
  fi
fi

for t in "${TRANSPORTS[@]}"; do
  if [[ "${t}" == "mqtt" ]]; then
    for b in "${MQTT_BROKERS_ARR[@]}"; do
      IFS=: read -r bname bhost bport <<<"${b}"
      svc=""; run_status_main=0
      if [[ ${SEQUENTIAL} -eq 1 ]]; then svc="$(get_services "${bname}")"; fi
      if [[ ${SEQUENTIAL} -eq 1 && -n "${svc}" ]]; then
        manage_service up "${svc}" || run_status_main=$?
        if (( run_status_main == 0 )); then wait_for_port "${bhost}" "${bport}" || run_status_main=$?; fi
      fi
      if (( run_status_main == 0 )); then run_single_execution "mqtt" "${bname}" "${bhost}" "${bport}" || run_status_main=$?; fi
      if [[ ${SEQUENTIAL} -eq 1 && -n "${svc}" ]]; then manage_service down "${svc}" || true; fi
      if (( run_status_main != 0 )); then record_run_status "${run_status_main}"; else cooldown_if_more_runs "mqtt/${bname}"; fi
      if (( STOP_AFTER_CURRENT != 0 )); then break; fi
    done
  elif [[ "${t}" == "amqp" ]]; then
    for b in "${AMQP_BROKERS_ARR[@]}"; do
      IFS=: read -r bname bhost bport <<<"${b}"
      svc=""; run_status_main=0
      if [[ ${SEQUENTIAL} -eq 1 ]]; then svc="$(get_services "${bname}-amqp")"; fi
      if [[ ${SEQUENTIAL} -eq 1 && -n "${svc}" ]]; then
        manage_service up "${svc}" || run_status_main=$?
        if (( run_status_main == 0 )); then wait_for_port "${bhost}" "${bport}" || run_status_main=$?; fi
      fi
      if (( run_status_main == 0 )); then run_single_execution "amqp" "${bname}" "${bhost}" "${bport}" || run_status_main=$?; fi
      if [[ ${SEQUENTIAL} -eq 1 && -n "${svc}" ]]; then manage_service down "${svc}" || true; fi
      if (( run_status_main != 0 )); then record_run_status "${run_status_main}"; else cooldown_if_more_runs "amqp/${bname}"; fi
      if (( STOP_AFTER_CURRENT != 0 )); then break; fi
    done
  else
    svc=""; port=""; run_status_main=0
    if [[ ${SEQUENTIAL} -eq 1 ]]; then svc="$(get_services "${t}")"; port="$(get_standard_port "${t}")"; fi
    if [[ ${SEQUENTIAL} -eq 1 && -n "${svc}" ]]; then
      manage_service up "${svc}" || run_status_main=$?
      if (( run_status_main == 0 )) && [[ -n "${port}" ]]; then wait_for_port "${HOST:-127.0.0.1}" "${port}" || run_status_main=$?; fi
    fi
    if (( run_status_main == 0 )); then run_single_execution "${t}" || run_status_main=$?; fi
    if [[ ${SEQUENTIAL} -eq 1 && -n "${svc}" ]]; then manage_service down "${svc}" || true; fi
    if (( run_status_main != 0 )); then record_run_status "${run_status_main}"; else cooldown_if_more_runs "${t}"; fi
  fi
  if (( STOP_AFTER_CURRENT != 0 )); then break; fi
done

write_phase_rate_summary

log "Plotting bursty fan-out results to ${PLOTS_DIR}"
plot_args=(--summary "${SUMMARY_CSV}" --out-dir "${PLOTS_DIR}" --profile "${RATE_PROFILE}" --latex)
if [[ ${APPEND_LATEST} -eq 1 || "${PHASE_OFFSET_SECONDS:-0}" != "0" ]]; then
  plot_args+=(--rebuild-plot-points)
fi
if [[ "${PLOT_BUCKET_SECONDS}" != "1" && "${PLOT_BUCKET_SECONDS}" != "1.0" ]]; then
  plot_args+=(--bucket-seconds "${PLOT_BUCKET_SECONDS}")
fi
if [[ "${DRY_RUN}" = 1 ]]; then
  printf '+ python3 %q' "${SCRIPT_DIR}/plot_bursty_fanout.py"
  printf ' %q' "${plot_args[@]}"
  printf '\n'
else
  python3 "${SCRIPT_DIR}/plot_bursty_fanout.py" "${plot_args[@]}"
fi

log "Done. Phase summary CSV: ${SUMMARY_CSV}"
if (( RUN_FAILURES != 0 )); then
  log "Experiment stopped early or failed; exiting with status ${RUN_FAILURES} after plotting"
  exit "${RUN_FAILURES}"
fi
