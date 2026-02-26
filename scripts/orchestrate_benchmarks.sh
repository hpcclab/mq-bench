#!/usr/bin/env bash
set -euo pipefail

# Orchestrate transport benchmarks over a parameter sweep and plot comparisons.
#
# Usage:
#   scripts/orchestrate_benchmarks.sh [--payloads "128 512 1024 4096"] \
#     [--rates "1000 5000 10000"] [--duration 20] [--snapshot 5] \
#     [--transports "zenoh mqtt redis nats rabbitmq rabbitmq-amqp rabbitmq-mqtt mqtt-mosquitto mqtt-emqx mqtt-hivemq mqtt-rabbitmq mqtt-artemis artemis amqp"] [--mqtt-brokers "mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885"] \
#     [--host 192.168.0.254] [--ssh-target user@host] [--ssh-user ubuntu] [--remote-dir ~/mq-bench] \
#     [--fanout] [--fanout-subs "2 4 8"] [--fanout-rates "1000 5000 10000"] \
#     [--start-services] [--run-id-prefix PREFIX] [--summary PATH] [--plots-only] [--dry-run] [--no-baseline]
#
# Notes:
# - Requires: bash, cargo (for run_* scripts), python3 with matplotlib (for plots).
# - Produces: results/benchmark_<timestamp>/{raw_data,plots}/
# - For remote brokers: use --host or --ssh-target to specify the remote host. Docker stats
#   will be collected via SSH from the remote host.
#
# Examples:
# - Full baseline sweep (zenoh+mqtt+redis) with default payloads/rates:
#   scripts/orchestrate_benchmarks.sh --start-services
# - Baseline sweep with custom payloads/rates and 30s duration:
#   scripts/orchestrate_benchmarks.sh --payloads "256 1024 4096" --rates "1000 5000 10000" --duration 30
# - MQTT across multiple brokers (override hosts/ports):
#   scripts/orchestrate_benchmarks.sh --transports "mqtt" --mqtt-brokers "mosq:127.0.0.1:1883 emqx:127.0.0.1:1884 hive:127.0.0.1:1885"
# - Point all brokers to a remote host (same IP for all; default ports retained):
#   HOST=192.168.0.254 scripts/orchestrate_benchmarks.sh --start-services=0
#   or equivalently:
#   scripts/orchestrate_benchmarks.sh --host 192.168.0.254 --start-services=0
# - Remote brokers with SSH-based docker stats collection:
#   scripts/orchestrate_benchmarks.sh --host 192.168.0.254 --ssh-target ubuntu@192.168.0.254
#   or using ssh-target which also infers host:
#   scripts/orchestrate_benchmarks.sh --ssh-target ubuntu@192.168.0.254
# - Fanout only (no baselines), sweep subscribers and use baseline rates:
#   scripts/orchestrate_benchmarks.sh --no-baseline --fanout --fanout-subs "2 4 8 16"
# - Fanout with dedicated rates and payload set:
#   scripts/orchestrate_benchmarks.sh --no-baseline --fanout --fanout-subs "2 8 32" --fanout-rates "2000 8000" --payloads "1024"
# - Plots only from the latest summary (no new runs):
#   scripts/orchestrate_benchmarks.sh --plots-only
# - Plots only from a specific summary file:
#   scripts/orchestrate_benchmarks.sh --plots-only --summary results/benchmark_YYYYMMDD_HHMMSS/raw_data/summary.csv
# - Dry-run (print actions without executing):
#   scripts/orchestrate_benchmarks.sh --dry-run --payloads "128 512" --rates "1000 5000"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# PAYLOADS=(128 1024 4096 16384)
PAYLOADS=(1024 4096 16384)
RATES=(5000 10000 50000 100000 200000 400000)
DURATION=20
SNAPSHOT=5
TRANSPORTS=(zenoh zenoh-mqtt zenoh-peer redis nats rabbitmq mqtt)
RUN_ID_PREFIX="bench"
START_SERVICES=0
PLOTS_ONLY=0
DRY_RUN=${DRY_RUN:-0}
SUMMARY_OVERRIDE=""
HOST="${HOST:-}"
INTERVAL_SEC=15

# Sequential / Remote execution
SEQUENTIAL=0
SSH_TARGET=""
REMOTE_DIR="~/mq-bench"
SSH_USER="${SSH_USER:-ubuntu}"

# Space-separated list of name:host:port tokens for MQTT
MQTT_BROKERS="mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885 rabbitmq:127.0.0.1:1886 artemis:127.0.0.1:1887"
declare -a MQTT_BROKERS_ARR=()

# Fanout controls
FANOUT_ENABLE=0
FANOUT_SUBS="4 16 64 128 256 512 1024"
FANOUT_RATES="1000"  # defaults to RATES if empty
BASELINE_ENABLE=1

usage() {
  sed -n '1,40p' "$0" | sed -n 's/^# \{0,1\}//p'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --payloads)
      shift; IFS=' ' read -r -a PAYLOADS <<<"${1:-}" ;;
    --rates)
      shift; IFS=' ' read -r -a RATES <<<"${1:-}" ;;
    --duration)
      shift; DURATION=${1:-20} ;;
    --snapshot)
      shift; SNAPSHOT=${1:-5} ;;
    --transports)
      shift; IFS=' ' read -r -a TRANSPORTS <<<"${1:-}" ;;
    --run-id-prefix)
      shift; RUN_ID_PREFIX=${1:-bench} ;;
    --start-services)
      START_SERVICES=1 ;;
    --plots-only)
      PLOTS_ONLY=1 ;;
    --summary)
      shift; SUMMARY_OVERRIDE=${1:-} ;;
    --mqtt-brokers)
      shift; MQTT_BROKERS=${1:-} ;;
    --host)
      shift; HOST=${1:-} ;;
    --ssh-target)
      shift; SSH_TARGET=${1:-} ;;
    --ssh-user)
      shift; SSH_USER=${1:-ubuntu} ;;
    --remote-dir)
      shift; REMOTE_DIR=${1:-} ;;
    --interval-sec)
      shift; INTERVAL_SEC=${1:-15} ;;
    --sequential)
      SEQUENTIAL=1 ;;
    --fanout)
      FANOUT_ENABLE=1 ;;
    --fanout-subs)
      shift; FANOUT_SUBS=${1:-} ;;
    --fanout-rates)
      shift; FANOUT_RATES=${1:-} ;;
    --no-baseline)
      BASELINE_ENABLE=0 ;;
    --dry-run)
      DRY_RUN=1 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
  shift || true
done

# Infer HOST from SSH_TARGET if not set
if [[ -z "${HOST}" ]] && [[ -n "${SSH_TARGET}" ]]; then
  # Extract host part from user@host or just host
  if [[ "${SSH_TARGET}" == *"@"* ]]; then
    HOST="${SSH_TARGET#*@}"
  else
    HOST="${SSH_TARGET}"
  fi
  echo "[$(date +%H:%M:%S)] Inferred HOST=${HOST} from SSH_TARGET"
fi

# Materialize MQTT brokers array
IFS=' ' read -r -a MQTT_BROKERS_ARR <<<"${MQTT_BROKERS}"

# If a global HOST is specified, rewrite MQTT broker hostnames to use it (keeping ports and names)
if [[ -n "${HOST}" ]]; then
  declare -a _REWRITTEN=()
  for tok in "${MQTT_BROKERS_ARR[@]}"; do
    IFS=: read -r bname _bhost bport <<<"${tok}"
    _REWRITTEN+=("${bname}:${HOST}:${bport}")
  done
  MQTT_BROKERS_ARR=("${_REWRITTEN[@]}")
fi

timestamp() { date +%Y%m%d_%H%M%S; }

BENCH_TS=$(timestamp)
BENCH_DIR="${REPO_ROOT}/results/benchmark_${BENCH_TS}"
RAW_DIR="${BENCH_DIR}/raw_data"
PLOTS_DIR="${BENCH_DIR}/plots"
mkdir -p "${RAW_DIR}" "${PLOTS_DIR}"

SUMMARY_CSV="${RAW_DIR}/summary.csv"
if [[ -n "${SUMMARY_OVERRIDE}" ]]; then
  SUMMARY_CSV="${SUMMARY_OVERRIDE}"
fi
# Write header if file is new/empty and we're going to append runs
if [[ ${PLOTS_ONLY} -eq 0 ]]; then
  if [[ ! -s "${SUMMARY_CSV}" ]]; then
    echo "transport,payload,rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes" > "${SUMMARY_CSV}"
  fi
fi

log() { echo "[$(date +%H:%M:%S)] $*"; }
run() { if [[ "$DRY_RUN" = 1 ]]; then echo "+ $*"; else eval "$*"; fi }

ensure_services() {
  if [[ $START_SERVICES -eq 1 ]]; then
    if [[ -n "${HOST}" ]] && [[ "${HOST}" != "127.0.0.1" ]] && [[ "${HOST}" != "localhost" ]]; then
      log "HOST=${HOST} indicates remote brokers; skipping local service startup"
    else
      log "Starting services via compose_up.sh"
      run "bash \"${SCRIPT_DIR}/compose_up.sh\""
    fi
  else
    log "Skipping service startup (use --start-services to enable)"
  fi
}

parse_and_append_summary() {
  local transport="$1" payload="$2" rate="$3" run_id="$4" art_dir="$5"
  local sub_csv pub_csv last_sub last_pub
  sub_csv="${art_dir}/sub.csv"
  pub_csv="${art_dir}/pub.csv"
  # Fallback to sub_agg.csv if present (fanout)
  if [[ ! -f "${sub_csv}" ]]; then
    if [[ -f "${art_dir}/sub_agg.csv" ]]; then
      sub_csv="${art_dir}/sub_agg.csv"
    else
      log "WARN: Missing ${sub_csv}; skipping row"
      return 0
    fi
  fi
  # Read last rows (skip header)
  last_sub=$(tail -n +2 "${sub_csv}" | tail -n1 || true)
  last_pub=$(tail -n +2 "${pub_csv}" 2>/dev/null | tail -n1 || true)
  if [[ -z "${last_sub}" ]]; then
    log "WARN: Empty sub.csv for ${run_id}"; return 0
  fi
  # Expect: ts,sent,recv,errors,tps,itps,p50,p95,p99,min,max,mean,connections,active_connections
  local _ts _sent recv _err tps _it p50 p95 p99 _min _max _mean _conns _active
  IFS=, read -r _ts _sent recv _err tps _it p50 p95 p99 _min _max _mean _conns _active <<<"${last_sub}"
  # Publisher total tps (optional)
  local pub_tps sent
  pub_tps=""
  sent="${_sent}"
  if [[ -n "${last_pub}" ]]; then
    # pub: ts,sent,recv,errors,tt,it,...,connections,active_connections
    local _pts psent _prev _perr ptt _pit _a _b _c _d _e _f _pconns _pactive
    IFS=, read -r _pts psent _prev _perr ptt _pit _a _b _c _d _e _f _pconns _pactive <<<"${last_pub}"
    pub_tps="${ptt}"
    sent="${psent}"
  fi
  local errors
  errors="${_err}"
  # Convert ns to ms for p50/95/99 if numeric
  ns_to_ms() { awk -v n="$1" 'BEGIN{if(n==""||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}'; }
  local p50_ms p95_ms p99_ms
  p50_ms=$(ns_to_ms "${p50}")
  p95_ms=$(ns_to_ms "${p95}")
  p99_ms=$(ns_to_ms "${p99}")
  # Aggregate docker stats if available (compute precise mem% from used/total)
  local stats_csv="${art_dir}/docker_stats.csv" max_cpu max_mem_perc max_mem_used
  max_cpu=""; max_mem_perc=""; max_mem_used=""
  if [[ -f "${stats_csv}" ]]; then
    local agg
    agg=$(awk -F, '
      function bytes(v,   n,u,mult,parts){
        gsub(/^[ \t]+|[ \t]+$/, "", v);
        if (match(v, /^([0-9.]+)([A-Za-z]+)$/, parts)) {
          n=parts[1]; u=parts[2];
          if (u=="B")   mult=1;
          else if (u=="kB"||u=="KB") mult=1000;
          else if (u=="MB") mult=1000*1000;
          else if (u=="GB") mult=1000*1000*1000;
          else if (u=="TB") mult=1000*1000*1000*1000;
          else if (u=="KiB") mult=1024;
          else if (u=="MiB") mult=1024*1024;
          else if (u=="GiB") mult=1024*1024*1024;
          else if (u=="TiB") mult=1024*1024*1024*1024;
          else mult=1;
          return n*mult;
        }
        return 0;
      }
      NR==1 {
        # Detect format: remote collector (5 cols) vs local (many cols)
        is_remote=(NF==5 && $3=="cpu_perc");
        # Detect presence of extended columns produced by jq sampler
        has_memprec = (NF>=12);  # mem_used_b(10), mem_limit_b(11), mem_perc_calc(12)
        has_cpuprec = (NF>=17);  # cpu_perc_num at 17
        next;
      }
      {
        if (is_remote) {
           # Remote format: timestamp,container,cpu_perc,mem_perc,mem_usage
           c=$3; gsub(/%/,"",c); c+=0
           perc=$4; gsub(/%/,"",perc); perc+=0
           used_str=$5; 
           # Parse mem usage like "12.5MiB / 1.2GiB" -> take first part
           split(used_str, parts, " ");
           used_part = parts[1];
           
           val=used_part+0; 
           unit=used_part; gsub(/[0-9.]/,"",unit);
           
           if (index(unit,"Gi")>0) val*=1024*1024*1024;
           else if (index(unit,"Mi")>0) val*=1024*1024;
           else if (index(unit,"Ki")>0) val*=1024;
           used_b=val
        } else {
          # CPU: prefer numeric cpu_perc_num if present; else parse $4
          if (has_cpuprec) { c = $17 + 0; } else { c=$4; gsub(/%/,"",c); c+=0; }

          # Memory
          if (has_memprec) {
            used_b = $10 + 0; tot_b = $11 + 0;
            if (used_b>mused) mused=used_b;
            if (tot_b>0) {
              perc = $12 + 0; if (perc==0) perc = (used_b/tot_b)*100.0;
            }
          } else {
            split($5, mparts, " / ");
            used_b=bytes(mparts[1]); tot_b=bytes(mparts[2]);
            if (tot_b>0) { perc=(used_b/tot_b)*100.0; }
          }
        }
        
        if (c>mcpu) mcpu=c;
        if (perc>mperc) mperc=perc;
        if (used_b>mused) mused=used_b;
      }
      END{
        if (mcpu=="") mcpu=0;
        if (mperc=="") mperc=0;
        if (mused=="") mused=0;
        printf("%.6f,%.6f,%.0f", mcpu, mperc, mused);
      }
    ' "${stats_csv}" || true)
    IFS=, read -r max_cpu max_mem_perc max_mem_used <<<"${agg}"
  fi
  echo "${transport},${payload},${rate},${run_id},${tps},${p50_ms},${p95_ms},${p99_ms},${pub_tps},${sent},${recv},${errors},${art_dir},${max_cpu},${max_mem_perc},${max_mem_used}" >> "${SUMMARY_CSV}"
}

# Start remote docker stats collection (returns PID in REMOTE_STATS_PID global)
REMOTE_STATS_PID=""
start_remote_stats() {
  local art_dir="$1"
  REMOTE_STATS_PID=""
  if [[ -z "${HOST}" ]] || [[ "${HOST}" == "127.0.0.1" ]] || [[ "${HOST}" == "localhost" ]]; then
    return 0
  fi
  local stats_csv="${art_dir}/docker_stats.csv"
  mkdir -p "${art_dir}"
  log "Starting remote stats collector on ${HOST}..."
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ ${SCRIPT_DIR}/collect_remote_docker_stats.sh ${HOST} ${stats_csv} ${DURATION} ${SSH_USER} &"
  else
    "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${HOST}" "${stats_csv}" "${DURATION}" "${SSH_USER}" >/dev/null 2>&1 &
    REMOTE_STATS_PID=$!
  fi
}

# Stop remote docker stats collection
stop_remote_stats() {
  if [[ -n "${REMOTE_STATS_PID}" ]]; then
    if [[ "${DRY_RUN}" = 1 ]]; then
      echo "+ kill ${REMOTE_STATS_PID}"
    else
      kill "${REMOTE_STATS_PID}" 2>/dev/null || true
      wait "${REMOTE_STATS_PID}" 2>/dev/null || true
    fi
    REMOTE_STATS_PID=""
  fi
}

run_fanout_combo() {
  local transport="$1" subs="$2" payload="$3" rate="$4"
  local rid env_common env_host cmd art_dir
  env_common="SUBS=${subs} PAYLOAD=${payload} RATE=${rate} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT} KEY=bench/topic"
  case "${transport}" in
    zenoh)
      rid="${RUN_ID_PREFIX}_$(timestamp)_fanout_${transport}_s${subs}_p${payload}_r${rate}"
      art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
      env_host=""
      if [[ -n "${HOST}" ]]; then
        local _epsub="${ENDPOINT_SUB:-tcp/${HOST}:7447}"
        local _eppub="${ENDPOINT_PUB:-tcp/${HOST}:7447}"
        env_host="ENDPOINT_SUB=${_epsub} ENDPOINT_PUB=${_eppub}"
      fi
      cmd="ENGINE=zenoh ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      log "Running: fanout transport=zenoh, subs=${subs}, payload=${payload}, rate=${rate} (run_id=${rid})"
      start_remote_stats "${art_dir}"
      run "${cmd}"
      stop_remote_stats
      parse_and_append_summary "fanout-zenoh-s${subs}" "${payload}" "${rate}" "${rid}" "${art_dir}"
      ;;
    zenoh-mqtt)
      # Alias transport: use zenoh engine with same endpoints, distinct label
      rid="${RUN_ID_PREFIX}_$(timestamp)_fanout_${transport}_s${subs}_p${payload}_r${rate}"
      art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
      env_host=""
      if [[ -n "${HOST}" ]]; then
        local _epsub="${ENDPOINT_SUB:-tcp/${HOST}:7447}"
        local _eppub="${ENDPOINT_PUB:-tcp/${HOST}:7447}"
        env_host="ENDPOINT_SUB=${_epsub} ENDPOINT_PUB=${_eppub}"
      fi
      cmd="ENGINE=zenoh ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      log "Running: fanout transport=zenoh-mqtt, subs=${subs}, payload=${payload}, rate=${rate} (run_id=${rid})"
      start_remote_stats "${art_dir}"
      run "${cmd}"
      stop_remote_stats
      parse_and_append_summary "fanout-zenoh-mqtt-s${subs}" "${payload}" "${rate}" "${rid}" "${art_dir}"
      ;;
    zenoh-peer)
      rid="${RUN_ID_PREFIX}_$(timestamp)_fanout_${transport}_s${subs}_p${payload}_r${rate}"
      art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
      env_host=""
      if [[ -n "${HOST}" ]]; then
        local _epsub="${ENDPOINT_SUB:-tcp/${HOST}:7447}"
        local _eppub="${ENDPOINT_PUB:-tcp/${HOST}:7447}"
        env_host="ENDPOINT_SUB=${_epsub} ENDPOINT_PUB=${_eppub}"
      fi
      cmd="ENGINE=zenoh-peer ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      log "Running: fanout transport=zenoh-peer, subs=${subs}, payload=${payload}, rate=${rate} (run_id=${rid})"
      start_remote_stats "${art_dir}"
      run "${cmd}"
      stop_remote_stats
      parse_and_append_summary "fanout-zenoh-peer-s${subs}" "${payload}" "${rate}" "${rid}" "${art_dir}"
      ;;
    redis)
      rid="${RUN_ID_PREFIX}_$(timestamp)_fanout_${transport}_s${subs}_p${payload}_r${rate}"
      art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
      env_host=""
      if [[ -n "${HOST}" ]]; then env_host="REDIS_URL=redis://${HOST}:6379"; fi
      cmd="ENGINE=redis ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      log "Running: fanout transport=redis, subs=${subs}, payload=${payload}, rate=${rate} (run_id=${rid})"
      start_remote_stats "${art_dir}"
      run "${cmd}"
      stop_remote_stats
      parse_and_append_summary "fanout-redis-s${subs}" "${payload}" "${rate}" "${rid}" "${art_dir}"
      ;;
    mqtt)
      # Iterate over configured brokers
      for tok in "${MQTT_BROKERS_ARR[@]}"; do
        IFS=: read -r bname bhost bport <<<"${tok}"
        rid="${RUN_ID_PREFIX}_$(timestamp)_fanout_${transport}_${bname}_s${subs}_p${payload}_r${rate}"
        art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
        cmd="ENGINE=mqtt BROKER_CONTAINER=${bname} MQTT_HOST=${bhost} MQTT_PORT=${bport} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
        log "Running: fanout transport=mqtt(${bname}), subs=${subs}, payload=${payload}, rate=${rate} (run_id=${rid})"
        start_remote_stats "${art_dir}"
        run "${cmd}"
        stop_remote_stats
        parse_and_append_summary "fanout-mqtt-${bname}-s${subs}" "${payload}" "${rate}" "${rid}" "${art_dir}"
      done
      return 0
      ;;
    rabbitmq)
      rid="${RUN_ID_PREFIX}_$(timestamp)_fanout_${transport}_s${subs}_p${payload}_r${rate}"
      art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
      env_host=""
      if [[ -n "${HOST}" ]]; then env_host="RABBITMQ_HOST=${HOST}"; fi
      cmd="ENGINE=rabbitmq ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      log "Running: fanout transport=rabbitmq(amqp), subs=${subs}, payload=${payload}, rate=${rate} (run_id=${rid})"
      start_remote_stats "${art_dir}"
      run "${cmd}"
      stop_remote_stats
      parse_and_append_summary "fanout-rabbitmq-s${subs}" "${payload}" "${rate}" "${rid}" "${art_dir}"
      ;;
    nats)
      rid="${RUN_ID_PREFIX}_$(timestamp)_fanout_${transport}_s${subs}_p${payload}_r${rate}"
      art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_singlesite"
      env_host=""
      if [[ -n "${HOST}" ]]; then env_host="NATS_HOST=${HOST}"; fi
      cmd="ENGINE=nats ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_fanout.sh\" \"${rid}\""
      log "Running: fanout transport=nats, subs=${subs}, payload=${payload}, rate=${rate} (run_id=${rid})"
      start_remote_stats "${art_dir}"
      run "${cmd}"
      stop_remote_stats
      parse_and_append_summary "fanout-nats-s${subs}" "${payload}" "${rate}" "${rid}" "${art_dir}"
      ;;
    *)
      log "Unknown fanout transport: ${transport}"; return 1 ;;
  esac
}

run_one_combo() {
  local transport="$1" payload="$2" rate="$3"
  local rid env_common env_host cmd art_dir
  env_common="PAYLOAD=${payload} RATE=${rate} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT} KEY=bench/topic"
  case "${transport}" in
    zenoh)
      rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_r${rate}"
      env_host=""
      if [[ -n "${HOST}" ]]; then
        local _epsub="${ENDPOINT_SUB:-tcp/${HOST}:7447}"
        local _eppub="${ENDPOINT_PUB:-tcp/${HOST}:7447}"
        env_host="ENDPOINT_SUB=${_epsub} ENDPOINT_PUB=${_eppub}"
      fi
      cmd="ENGINE=zenoh ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_baseline.sh\" \"${rid}\""
      art_dir="${REPO_ROOT}/artifacts/${rid}/local_baseline"
      ;;
    zenoh-mqtt)
      rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_r${rate}"
      env_host=""
      if [[ -n "${HOST}" ]]; then
        local _epsub="${ENDPOINT_SUB:-tcp/${HOST}:7447}"
        local _eppub="${ENDPOINT_PUB:-tcp/${HOST}:7447}"
        env_host="ENDPOINT_SUB=${_epsub} ENDPOINT_PUB=${_eppub}"
      fi
      cmd="ENGINE=zenoh ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_baseline.sh\" \"${rid}\""
      art_dir="${REPO_ROOT}/artifacts/${rid}/local_baseline"
      ;;
    zenoh-peer)
      rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_r${rate}"
      env_host=""
      if [[ -n "${HOST}" ]]; then
        local _epsub="${ENDPOINT_SUB:-tcp/${HOST}:7447}"
        local _eppub="${ENDPOINT_PUB:-tcp/${HOST}:7447}"
        env_host="ENDPOINT_SUB=${_epsub} ENDPOINT_PUB=${_eppub}"
      fi
      cmd="ENGINE=zenoh-peer ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_baseline.sh\" \"${rid}\""
      art_dir="${REPO_ROOT}/artifacts/${rid}/local_baseline"
      ;;
    mqtt)
      # Iterate over configured brokers
      for tok in "${MQTT_BROKERS_ARR[@]}"; do
        IFS=: read -r bname bhost bport <<<"${tok}"
        rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_${bname}_p${payload}_r${rate}"
        cmd="ENGINE=mqtt BROKER_CONTAINER=${bname} MQTT_HOST=${bhost} MQTT_PORT=${bport} ${env_common} bash \"${SCRIPT_DIR}/run_baseline.sh\" \"${rid}\""
        art_dir="${REPO_ROOT}/artifacts/${rid}/local_baseline"
        log "Running: transport=mqtt(${bname}), payload=${payload}, rate=${rate} (run_id=${rid})"
        start_remote_stats "${art_dir}"
        run "${cmd}"
        stop_remote_stats
        parse_and_append_summary "mqtt-${bname}" "${payload}" "${rate}" "${rid}" "${art_dir}"
      done
      return 0
      ;;
    redis)
      rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_r${rate}"
      env_host=""
      if [[ -n "${HOST}" ]]; then env_host="REDIS_URL=redis://${HOST}:6379"; fi
      cmd="ENGINE=redis ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_baseline.sh\" \"${rid}\""
      art_dir="${REPO_ROOT}/artifacts/${rid}/local_baseline"
      ;;
    rabbitmq)
      rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_r${rate}"
      env_host=""
      if [[ -n "${HOST}" ]]; then env_host="RABBITMQ_HOST=${HOST}"; fi
      cmd="ENGINE=rabbitmq ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_baseline.sh\" \"${rid}\""
      art_dir="${REPO_ROOT}/artifacts/${rid}/local_baseline"
      ;;
    nats)
      rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_r${rate}"
      env_host=""
      if [[ -n "${HOST}" ]]; then env_host="NATS_HOST=${HOST}"; fi
      cmd="ENGINE=nats ${env_host} ${env_common} bash \"${SCRIPT_DIR}/run_baseline.sh\" \"${rid}\""
      art_dir="${REPO_ROOT}/artifacts/${rid}/local_baseline"
      ;;
    *)
      log "Unknown transport: ${transport}"; return 1 ;;
  esac
  log "Running: transport=${transport}, payload=${payload}, rate=${rate} (run_id=${rid})"
  start_remote_stats "${art_dir}"
  run "${cmd}"
  stop_remote_stats
  parse_and_append_summary "${transport}" "${payload}" "${rate}" "${rid}" "${art_dir}"
}

plot_results() {
  local csv_path="$1" out_dir="$2"
  log "Plotting results to ${out_dir}"
  if [[ "$DRY_RUN" = 1 ]]; then
    echo "+ python3 - <<'PY' ..."
    return 0
  fi
  python3 "${SCRIPT_DIR}/plot_results.py" --summary "$csv_path" --out-dir "$out_dir"
}

main() {
  log "Benchmark root: ${BENCH_DIR}"
  ensure_services

  if [[ $PLOTS_ONLY -eq 0 ]]; then
    if [[ ${BASELINE_ENABLE} -eq 1 ]]; then
      for t in "${TRANSPORTS[@]}"; do
        for p in "${PAYLOADS[@]}"; do
          for r in "${RATES[@]}"; do
            run_one_combo "$t" "$p" "$r"
          done
        done
      done
    else
      log "Skipping baseline runs (--no-baseline)"
    fi

    if [[ ${FANOUT_ENABLE} -eq 1 ]]; then
      # Resolve fanout rates
      local -a frates
      if [[ -n "${FANOUT_RATES}" ]]; then
        IFS=' ' read -r -a frates <<<"${FANOUT_RATES}"
      else
        frates=("${RATES[@]}")
      fi
      local -a fsubs
      IFS=' ' read -r -a fsubs <<<"${FANOUT_SUBS}"
      for t in "${TRANSPORTS[@]}"; do
        for s in "${fsubs[@]}"; do
          for p in "${PAYLOADS[@]}"; do
            for r in "${frates[@]}"; do
              run_fanout_combo "$t" "$s" "$p" "$r"
            done
          done
        done
      done
    fi
  else
    # In plots-only, if no summary provided and our summary is empty, try latest one
    if [[ -z "${SUMMARY_OVERRIDE}" ]] && { [[ ! -f "${SUMMARY_CSV}" ]] || [[ $(wc -l <"${SUMMARY_CSV}") -le 1 ]]; }; then
      latest=$(ls -1dt "${REPO_ROOT}"/results/benchmark_*/raw_data/summary.csv 2>/dev/null | head -n1 || true)
      if [[ -n "${latest}" ]]; then
        SUMMARY_CSV="${latest}"
        log "Plots-only: using latest summary at ${SUMMARY_CSV}"
      else
        log "Plots-only: no summary CSV found; nothing to plot"
      fi
    else
      log "Plots-only mode: using ${SUMMARY_CSV}"
    fi
  fi

  plot_results "${SUMMARY_CSV}" "${PLOTS_DIR}"

  log "Done. Summary CSV: ${SUMMARY_CSV}"
}

main "$@"
