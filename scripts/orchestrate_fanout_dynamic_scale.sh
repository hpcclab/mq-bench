#!/usr/bin/env bash
set -euo pipefail

# Orchestrate Dynamic Fan-Out Scaling.
#
# Keeps the fan-out ratio fixed and changes the number of active publisher/topic
# groups across phases. All subscribers and publishers are started before the
# measurement profile begins; inactive publisher groups stay connected in
# zero-rate profile phases.
#
# Example:
#   SUB_RAMP_UP_SECS=60 nohup bash scripts/orchestrate_fanout_dynamic_scale.sh \
#     --host 192.168.0.245 \
#     --transports "zenoh nats redis mqtt" \
#     --subs 2000 \
#     --subs-per-pub 100 \
#     --sub-procs-per-topic 10 \
#     --ssh-target ubuntu@192.168.0.245 \
#     --remote-dir /home/ubuntu \
#     --payload 128B \
#     --group-rate 1000 \
#     --sequential \
#     --scale-profile "baseline:60:1,burst:120:5,recovery:90:1,burst:120:10,recovery:90:1,burst:120:20,recovery:120:1" \
#     > dynamic_fanout_scaling.log 2>&1 &

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SUBS=2000
SUBS_PER_PUB=100
SUB_PROCS_PER_TOPIC=${SUB_PROCS_PER_TOPIC:-1}
PAYLOAD_TOKEN="1024"
GROUP_RATE=1000
SCALE_PROFILE="baseline:60:1,burst:120:5,recovery:90:1,burst:120:10,recovery:90:1,burst:120:20,recovery:120:1"
SNAPSHOT=1
PUB_PRESTART_SECS="${PUB_PRESTART_SECS:-5}"
RUN_ID_PREFIX="fanout_dynamic_scale"
PLOT_BUCKET_SECONDS="${PLOT_BUCKET_SECONDS:-1}"
TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
DEFAULT_TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
HOST="${HOST:-}"
SEQUENTIAL=0
SSH_TARGET=""
REMOTE_DIR="~/mq-bench"
INTERVAL_SEC=${INTERVAL_SEC:-120}
DRY_RUN=${DRY_RUN:-0}
SUB_RAMP_UP_SECS="${SUB_RAMP_UP_SECS:-}"

TS=""
BENCH_DIR=""
RAW_DIR=""
PLOTS_DIR=""
SUMMARY_CSV=""
PROFILE_DIR=""
SUMMARY_RATE_PROFILE=""
PAYLOAD_BYTES=""
NUM_GROUPS=0
DURATION=0

log() { echo "[$(date +%H:%M:%S)] $*"; }
run() { if [[ "${DRY_RUN}" = 1 ]]; then echo "+ $*"; else eval "$*"; fi; }
timestamp() { date +%Y%m%d_%H%M%S; }

usage() {
  sed -n '1,48p' "$0" | sed -n 's/^# //p'
  cat <<'EOF'

Options:
  --host <broker-host>
  --transports "<transport-list>"
  --subs <total-subs>
  --subs-per-pub <subs-per-topic>
  --ssh-target <user@host>
  --remote-dir <remote-dir>
  --payload <payload-size>
  --group-rate <msg/s per active publisher>
  --scale-profile "<phase:duration:active_groups,...>"
  --sub-procs-per-topic <N>
  --sequential
  --dry-run
EOF
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

scale_profile_duration_secs() {
  local profile="$1"
  awk -v spec="${profile}" '
    BEGIN {
      n = split(spec, phases, ",")
      if (n < 1) exit 1
      for (i = 1; i <= n; i++) {
        m = split(phases[i], parts, ":")
        if (m != 3 || parts[2] !~ /^[0-9]+$/ || parts[2] <= 0 || parts[3] !~ /^[0-9]+$/ || parts[3] <= 0) exit 1
        total += parts[2]
      }
      print total
    }
  '
}

max_active_groups() {
  local profile="$1"
  awk -v spec="${profile}" '
    BEGIN {
      n = split(spec, phases, ",")
      for (i = 1; i <= n; i++) {
        split(phases[i], parts, ":")
        if (parts[3] + 0 > max) max = parts[3] + 0
      }
      print max
    }
  '
}

validate_config() {
  if [[ ! "${SUBS}" =~ ^[0-9]+$ ]] || (( SUBS <= 0 )); then echo "[error] Invalid --subs: ${SUBS}" >&2; exit 2; fi
  if [[ ! "${SUBS_PER_PUB}" =~ ^[0-9]+$ ]] || (( SUBS_PER_PUB <= 0 )); then echo "[error] Invalid --subs-per-pub: ${SUBS_PER_PUB}" >&2; exit 2; fi
  if (( SUBS % SUBS_PER_PUB != 0 )); then echo "ERROR: --subs must be divisible by --subs-per-pub" >&2; exit 1; fi
  if [[ ! "${GROUP_RATE}" =~ ^[0-9]+([.][0-9]+)?$ ]] || ! awk -v v="${GROUP_RATE}" 'BEGIN{exit !(v > 0)}'; then echo "[error] Invalid --group-rate: ${GROUP_RATE}" >&2; exit 2; fi
  if [[ ! "${PUB_PRESTART_SECS}" =~ ^[0-9]+$ ]]; then echo "[error] Invalid PUB_PRESTART_SECS: ${PUB_PRESTART_SECS}" >&2; exit 2; fi
  NUM_GROUPS=$(( SUBS / SUBS_PER_PUB ))
  local max_groups
  max_groups="$(max_active_groups "${SCALE_PROFILE}")"
  if (( max_groups > NUM_GROUPS )); then
    echo "ERROR: max active groups (${max_groups}) exceeds total publisher groups (${NUM_GROUPS})" >&2
    exit 1
  fi
  if ! DURATION="$(scale_profile_duration_secs "${SCALE_PROFILE}")"; then echo "[error] Invalid --scale-profile: ${SCALE_PROFILE}" >&2; exit 2; fi
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
    mqtt) echo "mosquitto" ;;
    *) echo "" ;;
  esac
}

get_standard_port() {
  case "$1" in
    zenoh) echo "7447" ;;
    redis) echo "6379" ;;
    nats) echo "4222" ;;
    rabbitmq) echo "5672" ;;
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
    if [[ -z "${broker_host}" || "${broker_host}" == "${HOST}" ]]; then echo "${SSH_TARGET}"; return 0; fi
  fi
  echo "${broker_host:-${HOST}}"
}

docker_compose_cmd() {
  local action="$1" services="${2:-}" cmd="docker compose ${action}"
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
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; then log "Port ${host}:${port} is open."; return 0; fi
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
    log "Cooldown after ${label}: waiting ${INTERVAL_SEC}s"
    if [[ "${DRY_RUN}" = 1 ]]; then echo "+ sleep ${INTERVAL_SEC}"; else sleep "${INTERVAL_SEC}"; fi
  fi
}

manage_service() {
  local action="$1" services="$2"
  if [[ ${SEQUENTIAL} -eq 0 || -z "${services}" ]]; then return 0; fi
  if [[ "${action}" == "up" ]]; then
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
  TS="$(timestamp)"
  BENCH_DIR="${REPO_ROOT}/results/fanout_dynamic_scale_${TS}"
  RAW_DIR="${BENCH_DIR}/raw_data"
  PLOTS_DIR="${BENCH_DIR}/plots"
  SUMMARY_CSV="${RAW_DIR}/summary_by_phase.csv"
  PROFILE_DIR="${RAW_DIR}/group_profiles"
  mkdir -p "${RAW_DIR}" "${PLOTS_DIR}" "${PROFILE_DIR}"
}

generate_profiles_and_summary() {
  local summary_profile_file="${RAW_DIR}/summary_rate_profile.txt"
  python3 - "${PROFILE_DIR}" "${BENCH_DIR}/phase-rate-summary.md" "${summary_profile_file}" "${SCALE_PROFILE}" "${NUM_GROUPS}" "${SUBS}" "${SUBS_PER_PUB}" "${PAYLOAD_BYTES}" "${GROUP_RATE}" "${PUB_PRESTART_SECS}" <<'PYGEN'
import os
import sys

profile_dir, summary_md, summary_profile_file, profile, groups_s, subs_s, subs_per_pub_s, payload_s, group_rate_s, prestart_s = sys.argv[1:]
groups = int(groups_s)
subs = int(subs_s)
subs_per_pub = int(subs_per_pub_s)
payload = int(payload_s)
group_rate = float(group_rate_s)
prestart = int(prestart_s)

raw_phases = []
burst_i = 0
recovery_i = 0
for raw in profile.split(','):
    parts = [p.strip() for p in raw.split(':')]
    if len(parts) != 3:
        raise SystemExit(f"invalid phase: {raw}")
    name, duration_s, active_s = parts
    duration = int(duration_s)
    active = int(active_s)
    lower = name.lower()
    if lower == 'baseline':
        label = f"Base ({active}g)"
    elif lower == 'burst':
        burst_i += 1
        label = f"B{burst_i} ({active}g)"
    elif lower == 'recovery':
        recovery_i += 1
        label = f"R{recovery_i} ({active}g)"
    else:
        label = f"{name} ({active}g)"
    raw_phases.append((name, label, duration, active))

os.makedirs(profile_dir, exist_ok=True)
for group_id in range(groups):
    items = []
    if prestart > 0:
        items.append(f"warmup:{prestart}:0")
    for _name, label, duration, active in raw_phases:
        rate = group_rate if group_id < active else 0.0
        items.append(f"{label}:{duration}:{rate:g}")
    with open(os.path.join(profile_dir, f"group_{group_id}.profile"), "w", encoding="utf-8") as fh:
        fh.write(','.join(items))

summary_items = []
if prestart > 0:
    summary_items.append(f"warmup:{prestart}:0")
for _name, label, duration, active in raw_phases:
    average_rate_per_group = (active * group_rate) / groups
    summary_items.append(f"{label}:{duration}:{average_rate_per_group:g}")
summary_profile = ','.join(summary_items)
with open(summary_profile_file, "w", encoding="utf-8") as fh:
    fh.write(summary_profile)

def fmt_rate(value):
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:g}k"
    return f"{value:g}"

with open(summary_md, "w", encoding="utf-8") as fh:
    fh.write("# Dynamic Fan-Out Scaling Phase Rates\n\n")
    fh.write(f"Source scale profile: `{profile}`\n\n")
    fh.write("## Run Setup\n\n")
    fh.write(f"- Total subscribers: `{subs}`\n")
    fh.write(f"- Subscribers per publisher/topic: `{subs_per_pub}`\n")
    fh.write(f"- Total publisher groups: `{groups}`\n")
    fh.write(f"- Payload: `{payload} bytes`\n")
    fh.write(f"- Group rate: `{group_rate:g} msg/s`\n")
    fh.write(f"- Publisher prestart: `{prestart} seconds`\n\n")
    fh.write("## Target Calculation\n\n")
    fh.write("```text\n")
    fh.write("active_publish_rate = active_groups * group_rate\n")
    fh.write("delivery_target = active_groups * group_rate * subs_per_pub\n")
    fh.write("```\n\n")
    fh.write("| Phase | Duration | Active groups | Active publishers | Active subscribers | Rate/pub | Total publish rate | Fan-out delivery target |\n")
    fh.write("|---|---:|---:|---:|---:|---:|---:|---:|\n")
    for _name, label, duration, active in raw_phases:
        active_subs = active * subs_per_pub
        total_pub = active * group_rate
        target = total_pub * subs_per_pub
        fh.write(
            f"| {label} | {duration}s | {active} | {active} | {active_subs} | "
            f"{fmt_rate(group_rate)} msg/s | {fmt_rate(total_pub)} msg/s | {fmt_rate(target)} msg/s |\n"
        )
PYGEN
  SUMMARY_RATE_PROFILE="$(<"${summary_profile_file}")"
  log "Wrote phase-rate summary: ${BENCH_DIR}/phase-rate-summary.md"
}

copy_run_raw_data() {
  local rid="$1" source_dir="$2" dest_dir="${RAW_DIR}/${rid}/fanout_singlesite"
  if [[ "${DRY_RUN}" = 1 ]]; then echo "+ cp -a ${source_dir}/. ${dest_dir}/" >&2; echo "${dest_dir}"; return 0; fi
  mkdir -p "${dest_dir}"
  cp -a "${source_dir}/." "${dest_dir}/"
  echo "${dest_dir}"
}

summarize_run() {
  local transport="$1" host="$2" port="$3" payload="$4" rid="$5" art_dir="$6"
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ python3 ${SCRIPT_DIR}/summarize_bursty_fanout.py --out ${SUMMARY_CSV} --transport ${transport} --host ${host} --port ${port} --payload ${payload} --subs ${SUBS} --pubs ${NUM_GROUPS} --subs-per-pub ${SUBS_PER_PUB} --profile ${SUMMARY_RATE_PROFILE} --run-id ${rid} --artifacts-dir ${art_dir}"
    return 0
  fi
  python3 "${SCRIPT_DIR}/summarize_bursty_fanout.py" \
    --out "${SUMMARY_CSV}" \
    --transport "${transport}" \
    --host "${host}" \
    --port "${port}" \
    --payload "${payload}" \
    --subs "${SUBS}" \
    --pubs "${NUM_GROUPS}" \
    --subs-per-pub "${SUBS_PER_PUB}" \
    --profile "${SUMMARY_RATE_PROFILE}" \
    --run-id "${rid}" \
    --artifacts-dir "${art_dir}"
}

run_single_execution() {
  local transport="$1"
  local payload="${PAYLOAD_BYTES}" sub_ramp_up_secs summary_host summary_port rid art_dir host_env monitor_env stats_container stats_pid stats_csv stats_duration raw_art_dir remote_stats_target
  cleanup_processes
  sub_ramp_up_secs="$(calc_sub_ramp_up_secs)"
  summary_host="${HOST:-127.0.0.1}"
  summary_port="$(get_standard_port "${transport}")"
  stats_container="$(get_services "${transport}")"
  rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_s${SUBS}_g${NUM_GROUPS}"
  art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
  mkdir -p "${art_dir}"

  profile_dir_q=$(printf '%q' "${PROFILE_DIR}")
  env_common="PUBS=${NUM_GROUPS} SUBS=${SUBS} SUBS_PER_PUB=${SUBS_PER_PUB} SUB_PROCS_PER_TOPIC=${SUB_PROCS_PER_TOPIC} CONTROLLED_FANOUT=1 SUB_RAMP_UP_SECS=${sub_ramp_up_secs} RATE_PROFILE_DIR=${profile_dir_q} PAYLOAD=${payload} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT}"
  host_env=""
  monitor_env=""
  if ! is_remote_host "${summary_host}" && [[ -n "${stats_container}" ]]; then monitor_env="MONITOR_CONTAINERS=${stats_container}"; fi

  stats_pid=0
  stats_csv="${art_dir}/docker_stats.csv"
  stats_duration=$(( DURATION + ${sub_ramp_up_secs%%.*} + 15 ))
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

  log "Run: transport=${transport} subs=${SUBS} groups=${NUM_GROUPS} payload=${payload}B scale_profile=${SCALE_PROFILE}"
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
      host_env="MQTT_HOST=${HOST:-127.0.0.1} MQTT_PORT=1883"
      run "${monitor_env} ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      ;;
    *) log "Unknown transport: ${transport}"; return 1 ;;
  esac

  if [[ -n "${stats_pid}" ]] && (( stats_pid > 0 )); then kill "${stats_pid}" 2>/dev/null || true; wait "${stats_pid}" 2>/dev/null || true; fi
  raw_art_dir="$(copy_run_raw_data "${rid}" "${art_dir}")"
  summarize_run "${transport}" "${summary_host}" "${summary_port}" "${payload}" "${rid}" "${raw_art_dir}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subs) shift; SUBS=${1:-2000} ;;
    --subs-per-pub) shift; SUBS_PER_PUB=${1:-100} ;;
    --sub-procs-per-topic) shift; SUB_PROCS_PER_TOPIC=${1:-1} ;;
    --payload) shift; PAYLOAD_TOKEN=${1:-1024} ;;
    --group-rate) shift; GROUP_RATE=${1:-1000} ;;
    --scale-profile) shift; SCALE_PROFILE="$(clean_quotes "${1:-}")" ;;
    --snapshot) shift; SNAPSHOT=${1:-1} ;;
    --transports) shift; if [[ -n "${1:-}" ]]; then IFS=' ' read -r -a TRANSPORTS <<<"${1}"; fi ;;
    --host) shift; HOST=${1:-} ;;
    --sequential) SEQUENTIAL=1 ;;
    --ssh-target) shift; SSH_TARGET=${1:-} ;;
    --remote-dir) shift; REMOTE_DIR=${1:-} ;;
    --interval-sec|--cooldown-sec) shift; INTERVAL_SEC=${1:-0} ;;
    --dry-run) DRY_RUN=1 ;;
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
validate_config
init_dirs
generate_profiles_and_summary

log "Dynamic fan-out scaling benchmark -> ${BENCH_DIR}"
DURATION=$(( DURATION + PUB_PRESTART_SECS ))
log "Resolved payload=${PAYLOAD_BYTES}B subs=${SUBS} subs_per_pub=${SUBS_PER_PUB} groups=${NUM_GROUPS} group_rate=${GROUP_RATE} duration=${DURATION}s (includes ${PUB_PRESTART_SECS}s publisher prestart)"
log "Scale profile: ${SCALE_PROFILE}"
log "Summary profile for plotting targets: ${SUMMARY_RATE_PROFILE}"

for t in "${TRANSPORTS[@]}"; do
  svc=""; port=""
  if [[ ${SEQUENTIAL} -eq 1 ]]; then svc="$(get_services "${t}")"; port="$(get_standard_port "${t}")"; fi
  if [[ ${SEQUENTIAL} -eq 1 && -n "${svc}" ]]; then manage_service up "${svc}"; if [[ -n "${port}" ]]; then wait_for_port "${HOST:-127.0.0.1}" "${port}"; fi; fi
  run_single_execution "${t}"
  if [[ ${SEQUENTIAL} -eq 1 && -n "${svc}" ]]; then manage_service down "${svc}"; fi
  cooldown_between_runs "${t}"
done

log "Plotting dynamic fan-out scaling results to ${PLOTS_DIR}"
plot_args=(--summary "${SUMMARY_CSV}" --out-dir "${PLOTS_DIR}" --profile "${SUMMARY_RATE_PROFILE}" --latex --throughput-y-scale 1000000 --throughput-y-label "Delivered throughput (million msg/s)")
if [[ "${PLOT_BUCKET_SECONDS}" != "1" && "${PLOT_BUCKET_SECONDS}" != "1.0" ]]; then plot_args+=(--bucket-seconds "${PLOT_BUCKET_SECONDS}"); fi
if [[ "${DRY_RUN}" = 1 ]]; then
  printf '+ python3 %q' "${SCRIPT_DIR}/plot_bursty_fanout.py"
  printf ' %q' "${plot_args[@]}"
  printf '\n'
else
  python3 "${SCRIPT_DIR}/plot_bursty_fanout.py" "${plot_args[@]}"
fi

log "Done. Phase summary CSV: ${SUMMARY_CSV}"
