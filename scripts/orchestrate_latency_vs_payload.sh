#!/usr/bin/env bash
set -euo pipefail

# Orchestrate latency-vs-payload experiment across transports using N pub/sub pairs.
#
# Topology: N publishers → N subscribers (pair per topic)
# Workload: payloads sweep; total send rate fixed; per-publisher rate = total_rate / N
# Runner: uses scripts/run_multi_topic_perkey.sh for N paired topics
# Output: results/latency_vs_payload_<ts>/{raw_data,plots}/ with summary.csv and figures
#
# Usage examples:
#   # Default: N=10, total_rate=5000/s, payloads 1KB 10KB 100KB 500KB 1MB across zenoh/redis/nats/rabbitmq/mqtt
#   scripts/orchestrate_latency_vs_payload.sh
#
#   # Remote host for all brokers (keep default ports)
#   scripts/orchestrate_latency_vs_payload.sh --host 192.168.0.254
#
#   # Sequential mode: start/stop containers one by one (local)
#   scripts/orchestrate_latency_vs_payload.sh --sequential
#
#   # Sequential mode on remote machine (via SSH)
#   scripts/orchestrate_latency_vs_payload.sh --sequential --ssh-target user@192.168.0.254 --remote-dir ~/mq-bench
#
#   # Customize transports and payloads
#   scripts/orchestrate_latency_vs_payload.sh \
#     --transports "zenoh redis nats rabbitmq mqtt" \
#     --payloads "1024 10240 102400 512000 1048576" \
#     --pairs 10 --total-rate 5000 --duration 30

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
PAIRS=10                # N pub/sub pairs
TOTAL_RATE=100         # msgs/s (system-wide)
DURATION=30             # seconds
SNAPSHOT=1
RUN_ID_PREFIX="latency"
# Include zenoh-mqtt as a distinct label (internally uses zenoh engine)
TRANSPORTS=(zenoh zenoh-mqtt redis nats rabbitmq mqtt)
# Default payloads (bytes): 1KB, 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB
PAYLOADS=(1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576)
PAYLOADS=(1024 16384 1048576)  # 1KB, 16KB, 1MB for quicker runs
# Keep immutable copies for fallback if user passes empty strings to flags
DEFAULT_TRANSPORTS=(zenoh zenoh-mqtt redis nats rabbitmq mqtt)
# DEFAULT_PAYLOADS=(1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152)
DEFAULT_PAYLOADS=(1024 16384 1048576)  # 1KB, 16KB, 1MB for quicker runs
START_SERVICES=0
DRY_RUN=${DRY_RUN:-0}
HOST="${HOST:-}" 
SUMMARY_OVERRIDE="${SUMMARY_OVERRIDE:-}"  # optional path to existing summary.csv to append
INTERVAL_SEC=65           # sleep between tests (TCP TIME_WAIT is 60s)
LATENCY_ONLY=0            # only capture latency columns in summary
WARMUP_SECS=0             # duration for warmup run
WARMUP_PAYLOAD=1024       # payload size for warmup
SKIP_SUMMARY=0            # internal flag to skip writing to summary.csv
RAMP_UP_SECS=5            # ramp-up time for pub/sub connections
APPEND_LATEST=0           # append to most recent results directory
IGNORE_START_SECS=5       # seconds to ignore from beginning of data
IGNORE_END_SECS=5         # seconds to ignore from end of data

# Sequential / Remote execution
SEQUENTIAL=0
SSH_TARGET=""
REMOTE_DIR="~/mq-bench"

# MQTT brokers (name:host:port). Default to local compose ports.
DEFAULT_MQTT_BROKERS="mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885 rabbitmq:127.0.0.1:1886 artemis:127.0.0.1:1887"
MQTT_BROKERS="${DEFAULT_MQTT_BROKERS}"
declare -a MQTT_BROKERS_ARR=()

# Treat suspicious quote-only tokens as empty (handles accidental smart quotes passed as values)
is_quote_only_token() {
  local v="$1"
  case "$v" in
    "\"\""|"''"|"“”"|"””"|"‘‘"|"’’") return 0;;
    *) return 1;;
  esac
}

# Helper to clean smart quotes from a string
clean_quotes() {
  local v="$1"
  # Remove leading/trailing smart quotes or normal quotes
  v="${v#[\"\'“”‘’]}"
  v="${v%[\"\'“”‘’]}"
  echo "$v"
}

# Convert payload tokens (possibly with KB suffix) to bytes
to_bytes() {
  local tok="$1"
  if [[ "$tok" =~ ^[0-9]+$ ]]; then
    echo "$tok"; return 0
  fi
  # Accept m/M/mb/MB suffix as mebibytes (x1024*1024)
  if [[ "$tok" =~ ^([0-9]+)[Mm][Bb]?$ ]]; then
    local n="${BASH_REMATCH[1]}"
    echo $(( n * 1024 * 1024 ))
    return 0
  fi
  # Accept k/K/kb/KB suffix as kibibytes (x1024)
  if [[ "$tok" =~ ^([0-9]+)[Kk][Bb]?$ ]]; then
    local n="${BASH_REMATCH[1]}"
    echo $(( n * 1024 ))
    return 0
  fi
  # Accept b/B suffix explicitly as bytes
  if [[ "$tok" =~ ^([0-9]+)[bB]$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  # Fallback: try as integer
  if [[ "$tok" =~ ^([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}"; return 0
  fi
  echo "$tok"  # return as-is; caller may handle
}

timestamp() { date +%Y%m%d_%H%M%S; }
# Directories are initialized after parsing CLI to honor overrides
TS=""
BENCH_DIR=""
RAW_DIR=""
PLOTS_DIR=""
SUMMARY_CSV=""

init_dirs() {
  # Handle --append-latest: find and use most recent results directory
  if [[ ${APPEND_LATEST} -eq 1 ]] && [[ -z "${SUMMARY_OVERRIDE}" ]] && [[ -z "${BENCH_DIR}" ]]; then
    local latest_dir
    latest_dir=$(ls -1d "${REPO_ROOT}/results/latency_vs_payload_"* 2>/dev/null | sort -r | head -1 || true)
    if [[ -n "${latest_dir}" ]] && [[ -d "${latest_dir}" ]]; then
      BENCH_DIR="${latest_dir}"
      RAW_DIR="${BENCH_DIR}/raw_data"
      PLOTS_DIR="${BENCH_DIR}/plots"
      # Extract timestamp from directory name
      TS="${latest_dir##*latency_vs_payload_}"
      log "Appending to existing run: ${BENCH_DIR}"
    else
      log "WARN: --append-latest specified but no existing latency_vs_payload_* directory found. Creating new."
    fi
  fi

  # Use existing TS if set, else generate
  if [[ -z "${TS}" ]]; then TS="$(timestamp)"; fi
  # Default bench dir if none provided via overrides
  if [[ -z "${BENCH_DIR}" ]]; then
    BENCH_DIR="${REPO_ROOT}/results/latency_vs_payload_${TS}"
  fi
  # Set defaults if not explicitly overridden later
  if [[ -z "${RAW_DIR}" ]]; then RAW_DIR="${BENCH_DIR}/raw_data"; fi
  if [[ -z "${PLOTS_DIR}" ]]; then PLOTS_DIR="${BENCH_DIR}/plots"; fi

  # Normalize to absolute paths
  case "${RAW_DIR}" in /*) ;; *) RAW_DIR="${REPO_ROOT}/${RAW_DIR}" ;; esac
  case "${PLOTS_DIR}" in /*) ;; *) PLOTS_DIR="${REPO_ROOT}/${PLOTS_DIR}" ;; esac

  mkdir -p "${RAW_DIR}" "${PLOTS_DIR}"

  # SUMMARY override: allow file or directory path
  if [[ -n "${SUMMARY_OVERRIDE}" ]]; then
    if [[ -d "${SUMMARY_OVERRIDE}" || "${SUMMARY_OVERRIDE}" != *.csv ]]; then
      # Treat as directory
      local dir="${SUMMARY_OVERRIDE}"
      case "${dir}" in /*) ;; *) dir="${REPO_ROOT}/${dir}" ;; esac
      mkdir -p "${dir}"
      SUMMARY_CSV="${dir}/summary.csv"
    else
      # Treat as explicit file path
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
  # Print only lines starting with "# " from the file header
  sed -n '1,120p' "$0" | sed -n 's/^# //p'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pairs)
      shift; PAIRS=${1:-10} ;;
    --total-rate)
      shift; TOTAL_RATE=${1:-5000} ;;
    --duration)
      shift; DURATION=${1:-30} ;;
    --snapshot)
      shift; SNAPSHOT=${1:-5} ;;
    --payloads)
      shift;
      if [[ -n "${1:-}" ]]; then
        # Accept space-, comma-, or newline-separated tokens
        _raw_payloads="${1}"
        # Replace commas with spaces
        _raw_payloads="${_raw_payloads//,/ }"
        # Split on any whitespace (space, tab, newline)
        IFS=$' \t\n' read -r -a PAYLOADS <<<"${_raw_payloads}"
        # Normalize KB/B suffixes to bytes
        _norm=()
        for tok in "${PAYLOADS[@]}"; do
          b=""
          b=$(to_bytes "$tok")
          if [[ "$b" =~ ^[0-9]+$ ]]; then
            _norm+=("$b")
          else
            echo "[warn] ignoring invalid payload token: '$tok'" >&2
          fi
        done
        PAYLOADS=("${_norm[@]}")
      else
        echo "[warn] --payloads given with empty value; keeping defaults: ${PAYLOADS[*]}" >&2
      fi
      ;;
    --transports)
      shift;
      if [[ -n "${1:-}" ]]; then
        IFS=' ' read -r -a TRANSPORTS <<<"${1}"
      else
        echo "[warn] --transports given with empty value; keeping defaults: ${TRANSPORTS[*]}" >&2
      fi
      ;;
    --run-id-prefix)
      shift; RUN_ID_PREFIX=${1:-latency} ;;
    --host)
      shift; HOST=${1:-} ;;
    --start-services)
      START_SERVICES=1 ;;
    --dry-run)
      DRY_RUN=1 ;;
    --summary)
      shift; SUMMARY_OVERRIDE=${1:-}
      if is_quote_only_token "${SUMMARY_OVERRIDE}"; then SUMMARY_OVERRIDE=""; fi ;;
    --out-dir)
      shift; PLOTS_DIR=${1:-}
      if is_quote_only_token "${PLOTS_DIR}"; then PLOTS_DIR=""; fi ;;
    --raw-dir)
      shift; RAW_DIR=${1:-}
      if is_quote_only_token "${RAW_DIR}"; then RAW_DIR=""; fi ;;
    --bench-dir)
      shift; BENCH_DIR=${1:-}
      if is_quote_only_token "${BENCH_DIR}"; then BENCH_DIR=""; fi ;;
    --mqtt-brokers)
      shift; 
      _raw_brokers="${1:-}"
      _raw_brokers=$(clean_quotes "${_raw_brokers}")
      MQTT_BROKERS="${_raw_brokers}" 
      ;;
    --interval-sec)
      shift; INTERVAL_SEC=${1:-0} ;;
    --latency-only)
      LATENCY_ONLY=1 ;;
    --warmup)
      shift; WARMUP_SECS=${1:-5} ;;
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

# Initialize directories and summary file now that CLI overrides are parsed
init_dirs

# Guardrails: if arrays were emptied by empty args, restore sensible defaults
if [[ ${#PAYLOADS[@]} -eq 0 ]]; then
  echo "[warn] No payloads specified after parsing; restoring defaults: ${DEFAULT_PAYLOADS[*]}" >&2
  PAYLOADS=("${DEFAULT_PAYLOADS[@]}")
fi
if [[ ${#TRANSPORTS[@]} -eq 0 ]]; then
  echo "[warn] No transports specified after parsing; restoring defaults: ${DEFAULT_TRANSPORTS[*]}" >&2
  TRANSPORTS=("${DEFAULT_TRANSPORTS[@]}")
fi

# Infer HOST from SSH_TARGET if not set
if [[ -z "${HOST}" ]] && [[ -n "${SSH_TARGET}" ]]; then
  # Extract host part from user@host or just host
  if [[ "${SSH_TARGET}" == *"@"* ]]; then
    HOST="${SSH_TARGET#*@}"
  else
    HOST="${SSH_TARGET}"
  fi
  log "Inferred HOST=${HOST} from SSH_TARGET"
fi

# Visibility: show resolved output and summary destinations
log "Resolved: SUMMARY_CSV=${SUMMARY_CSV} | PLOTS_DIR=${PLOTS_DIR} | RAW_DIR=${RAW_DIR}"
log "Resolved payloads (bytes): ${PAYLOADS[*]}"

# Materialize MQTT brokers array and apply HOST rewrite if needed
IFS=' ' read -r -a MQTT_BROKERS_ARR <<<"${MQTT_BROKERS}"

# If user provided only names (no host:port), try to resolve from defaults
declare -a _RESOLVED_BROKERS=()
for tok in "${MQTT_BROKERS_ARR[@]}"; do
  if [[ "${tok}" != *:* ]]; then
    # Look up in DEFAULT_MQTT_BROKERS
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
  for tok in "${MQTT_BROKERS_ARR[@]}"; do
    IFS=: read -r bname _bhost bport <<<"${tok}"
    _REWRITTEN+=("${bname}:${HOST}:${bport}")
  done
  MQTT_BROKERS_ARR=("${_REWRITTEN[@]}")
fi

# Header
if [[ ! -s "${SUMMARY_CSV}" ]]; then
  if [[ ${LATENCY_ONLY} -eq 1 ]]; then
    echo "transport,payload,rate,run_id,p50_ms,p95_ms,p99_ms" > "${SUMMARY_CSV}"
  else
    echo "transport,payload,rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes" > "${SUMMARY_CSV}"
  fi
fi

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

# Append steady-state metrics from a run directory into SUMMARY_CSV
append_summary_from_artifacts() {
  local transport="$1" payload="$2" rate="$3" run_id="$4" art_dir="$5"
  local sub_csv pub_csv
  sub_csv="${art_dir}/sub.csv"
  pub_csv="${art_dir}/pub.csv"
  if [[ ! -f "${sub_csv}" ]]; then
    if [[ -f "${art_dir}/sub_agg.csv" ]]; then
      sub_csv="${art_dir}/sub_agg.csv"
    else
      log "WARN: Missing subscriber CSV in ${art_dir}"; return 0
    fi
  fi
  if [[ ! -f "${pub_csv}" ]]; then
    if [[ -f "${art_dir}/mt_pub.csv" ]]; then
      pub_csv="${art_dir}/mt_pub.csv"
    fi
  fi
  
  # Calculate steady-state metrics by ignoring first IGNORE_START_SECS and last IGNORE_END_SECS
  # Columns: 1=timestamp, 2=sent_count, 3=received_count, 4=error_count, 5=total_throughput,
  #          6=interval_throughput, 7=p50, 8=p95, 9=p99, 10=min, 11=max, 12=mean, 13=connections, 14=active_connections
  local steady_state_metrics
  steady_state_metrics=$(awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
    NR == 1 { next }  # skip header
    {
        # First pass: collect all timestamps to determine time range
        ts = $1 + 0
        if (min_ts == "" || ts < min_ts) min_ts = ts
        if (max_ts == "" || ts > max_ts) max_ts = ts
        
        # Store row data for second pass
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
        # Calculate the valid time window
        window_start = min_ts + ignore_start
        window_end = max_ts - ignore_end
        
        # Second pass: filter rows within the time window
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
                
                # Accumulate latency values for averaging
                sum_p50 += row_p50[i]
                sum_p95 += row_p95[i]
                sum_p99 += row_p99[i]
                sum_mean += row_mean[i]
                count++
            }
        }
        
        if (count == 0) {
            # No steady-state rows found, output zeros
            print "0.00,0,0,0,0,0,0,0,0,0"
            exit
        }
        
        # Calculate TPS from delta: (last_recv - first_recv) / (last_ts - first_ts)
        duration = last_ts - first_ts
        if (duration > 0) {
            tps = (last_recv - first_recv) / duration
        } else {
            # Only 1 row matched, cannot compute delta TPS
            tps = 0
        }
        
        # Average latencies over steady-state period
        avg_p50 = sum_p50 / count
        avg_p95 = sum_p95 / count
        avg_p99 = sum_p99 / count
        
        # Delta counts during steady-state
        delta_recv = last_recv - first_recv
        delta_sent = last_sent - first_sent
        delta_err = last_err - first_err
        
        # Output includes first_ts and last_ts for docker stats filtering
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
        # Detect format: remote collector (5 cols) vs local (many cols)
        is_remote=(NF==5 && $3=="cpu_perc");
        has_memprec=(NF>=12); has_cpuprec=(NF>=17); 
        next 
      }
      {
        # Filter by steady-state timestamp range
        ts = $1 + 0
        if (start_ts > 0 && end_ts > 0) {
          if (ts < start_ts || ts > end_ts) next
        }
        
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
           # Local format
           if (has_cpuprec) { c=$17+0 } else { c=$4; gsub(/%/,"",c); c+=0 }
           if (has_memprec) {
             used_b=$10+0; tot_b=$11+0; if (used_b>mused) mused=used_b; perc=$12+0; if (perc==0 && tot_b>0) perc=(used_b/tot_b)*100.0; 
           }
        }
        
        # Max values
        if (c>mcpu) mcpu=c
        if (perc>mperc) mperc=perc
        if (used_b>mused) mused=used_b
        
        # Sum for averages
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

  if [[ ${LATENCY_ONLY} -eq 1 ]]; then
    echo "${transport},${payload},${rate},${run_id},${p50_ms},${p95_ms},${p99_ms}" >> "${SUMMARY_CSV}"
  else
    echo "${transport},${payload},${rate},${run_id},${tps},${p50_ms},${p95_ms},${p99_ms},${pub_tps},${sent},${recv},${_err},${art_dir},${max_cpu},${max_mem_perc},${max_mem_used}" >> "${SUMMARY_CSV}"
  fi
}

# Map transport/broker to docker-compose service names
get_services() {
  case "$1" in
    zenoh) echo "router1" ;;
    zenoh-mqtt) echo "router1" ;;
    redis) echo "redis" ;;
    nats) echo "nats" ;;
    rabbitmq) echo "rabbitmq" ;;
    mosquitto) echo "mosquitto" ;;
    emqx) echo "emqx" ;;
    hivemq) echo "hivemq" ;;
    artemis) echo "artemis" ;;
    *) echo "" ;;
  esac
}

# Get standard port for a transport
get_standard_port() {
  case "$1" in
    zenoh|zenoh-mqtt) echo "7447" ;;
    redis) echo "6379" ;;
    nats) echo "4222" ;;
    rabbitmq) echo "5672" ;;
    mosquitto) echo "1883" ;;
    emqx) echo "1884" ;;
    hivemq) echo "1885" ;;
    artemis) echo "1887" ;;
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
    # Remote execution
    local rcmd="cd ${REMOTE_DIR} && ${cmd}"
    log "[remote] ${SSH_TARGET}: ${rcmd}"
    if [[ "${DRY_RUN}" = 1 ]]; then
      echo "+ ssh ${SSH_TARGET} \"${rcmd}\""
    else
      ssh -o BatchMode=yes "${SSH_TARGET}" "${rcmd}"
    fi
  else
    # Local execution
    log "[local] ${cmd}"
    run "${cmd}"
  fi
}

# Kill any orphan mq-bench processes to ensure clean state
cleanup_processes() {
  log "Cleaning up orphan mq-bench processes..."
  pkill -f "mq-bench.*mt-sub" 2>/dev/null || true
  pkill -f "mq-bench.*mt-pub" 2>/dev/null || true
  pkill -f "mq-bench.*sub" 2>/dev/null || true
  pkill -f "mq-bench.*pub" 2>/dev/null || true
  # Brief pause to allow processes to terminate
  sleep 1
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

# Track which services have been started (for restart vs up logic)
declare -A STARTED_SERVICES=()

# Manage service lifecycle if sequential mode is enabled
manage_service() {
  local action="$1" # up, down, or restart
  local services="$2"
  if [[ $SEQUENTIAL -eq 0 ]]; then return 0; fi
  if [[ -z "$services" ]]; then return 0; fi
  
  if [[ "$action" == "up" ]]; then
    log "Starting services: ${services}"
    # Ensure clean state first
    cleanup_processes
    docker_compose_cmd down
    # Wait for connections to settle after stopping broker
    if [[ "${DRY_RUN}" != 1 ]]; then
      log "Waiting 10s for connections to settle..."
      sleep 10
    fi
    docker_compose_cmd "up -d" "$services"
    # Give broker time to fully initialize (not just open port)
    if [[ "${DRY_RUN}" != 1 ]]; then sleep 5; fi
    STARTED_SERVICES["$services"]=1
  elif [[ "$action" == "restart" ]]; then
    # Full restart (down+up) to clear broker state completely
    log "Full restarting services: ${services}"
    cleanup_processes
    docker_compose_cmd down
    # Wait for connections to settle after stopping broker
    if [[ "${DRY_RUN}" != 1 ]]; then
      log "Waiting 10s for connections to settle..."
      sleep 10
    fi
    docker_compose_cmd "up -d" "$services"
    # Give broker time to fully initialize (not just open port)
    if [[ "${DRY_RUN}" != 1 ]]; then sleep 5; fi
  elif [[ "$action" == "down" ]]; then
    log "Stopping services: ${services}"
    docker_compose_cmd down
  fi
}

# Execute one (transport, payload) combo
run_single_execution() {
  local transport="$1" payload="$2" pairs="$3" total_rate="$4"
  local broker_name="${5:-}" broker_host="${6:-}" broker_port="${7:-}"
  
  local per_rate=$(( total_rate / pairs ))
  local rid_suffix="${transport}_p${payload}_n${pairs}_r${per_rate}"
  if [[ -n "${broker_name}" ]]; then
    rid_suffix="${broker_name}_p${payload}_n${pairs}_r${per_rate}"
  fi
  
  if [[ ${SKIP_SUMMARY} -eq 1 ]]; then
    rid_suffix="${rid_suffix}_warmup"
  fi

  local rid="${RUN_ID_PREFIX}_$(timestamp)_${rid_suffix}"
  local art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_multi_topic_perkey"
  local env_common
  env_common="TENANTS=${pairs} REGIONS=1 SERVICES=1 SHARDS=1 SUBSCRIBERS=${pairs} PUBLISHERS=${pairs} PAYLOAD=${payload} RATE=${per_rate} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT} RAMP_UP_SECS=${RAMP_UP_SECS} TOPIC_PREFIX=bench/pair"

  local host_env=""
  local remote_stats_pid=""
  local stats_csv="${art_dir}/docker_stats.csv"

  # Start remote stats collector if HOST is set
  if [[ -n "${HOST}" ]]; then
    mkdir -p "${art_dir}"
    log "Starting remote stats collector on ${HOST}..."
    "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${HOST}" "${stats_csv}" "${DURATION}" >/dev/null 2>&1 &
    remote_stats_pid=$!
  fi

  case "${transport}" in
    zenoh)
      if [[ -n "${HOST}" ]]; then
        host_env="ENDPOINT_SUB=tcp/${HOST}:7447 ENDPOINT_PUB=tcp/${HOST}:7447"
      else
        host_env="ENDPOINT_SUB=tcp/127.0.0.1:7447 ENDPOINT_PUB=tcp/127.0.0.1:7447"
      fi
      run "ENGINE=zenoh ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    zenoh-mqtt)
      # Mixed engines via bridge: subscribers over MQTT on 1888 (bridge), publishers over zenoh on 7447
      if [[ -n "${HOST}" ]]; then
        host_env="ENGINE_SUB=mqtt MQTT_HOST=${HOST} MQTT_PORT=1888 ENGINE_PUB=zenoh ENDPOINT_PUB=tcp/${HOST}:7447"
      else
        host_env="ENGINE_SUB=mqtt MQTT_HOST=127.0.0.1 MQTT_PORT=1888 ENGINE_PUB=zenoh ENDPOINT_PUB=tcp/127.0.0.1:7447"
      fi
      run "${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    redis)
      if [[ -n "${HOST}" ]]; then host_env="REDIS_URL=redis://${HOST}:6379"; fi
      run "ENGINE=redis ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    nats)
      if [[ -n "${HOST}" ]]; then host_env="NATS_HOST=${HOST}"; fi
      run "ENGINE=nats ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    rabbitmq)
      if [[ -n "${HOST}" ]]; then host_env="RABBITMQ_HOST=${HOST}"; fi
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    mqtt)
      # For MQTT, we expect broker_name/host/port to be passed
      if [[ -z "${broker_name}" ]]; then
        log "ERROR: MQTT transport requires broker details"; return 1
      fi
      local br_transport="mqtt_${broker_name}"
      host_env="MQTT_HOST=${broker_host} MQTT_PORT=${broker_port}"
      
      # Inject credentials for Artemis (and potentially others if needed)
      if [[ "${broker_name}" == "artemis" ]]; then
        host_env="${host_env} MQTT_USERNAME=admin MQTT_PASSWORD=admin"
      fi

      run "ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      if [[ ${SKIP_SUMMARY} -eq 0 ]]; then
        append_summary_from_artifacts "${br_transport}" "${payload}" "${total_rate}" "${rid}" "${art_dir}"
      fi
      
      # Kill remote stats if started
      if [[ -n "${remote_stats_pid}" ]]; then kill "${remote_stats_pid}" 2>/dev/null || true; wait "${remote_stats_pid}" 2>/dev/null || true; fi
      return 0
      ;;
    *)
      log "Unknown transport: ${transport}"; return 1 ;;
  esac

  # Kill remote stats if started
  if [[ -n "${remote_stats_pid}" ]]; then kill "${remote_stats_pid}" 2>/dev/null || true; wait "${remote_stats_pid}" 2>/dev/null || true; fi

  if [[ ${SKIP_SUMMARY} -eq 0 ]]; then
    append_summary_from_artifacts "${transport}" "${payload}" "${total_rate}" "${rid}" "${art_dir}"
  fi
}

main() {
  log "Latency vs payload benchmark → ${BENCH_DIR}"
  
  # If NOT sequential, ensure services are up globally (legacy mode)
  if [[ $SEQUENTIAL -eq 0 ]]; then
    ensure_services
  fi

  for t in "${TRANSPORTS[@]}"; do
    if [[ "${t}" == "mqtt" ]]; then
      # MQTT: Iterate brokers
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

          for p in "${PAYLOADS[@]}"; do
            # Start or restart service before each iteration
            if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
               if [[ $first_iteration -eq 1 ]]; then
                  manage_service up "$svc"
                  first_iteration=0
               else
                  manage_service restart "$svc"
               fi
               wait_for_port "${bhost}" "${bport}"
               
               # Warmup after broker restart (skip if WARMUP_SECS=0)
               if [[ ${WARMUP_SECS} -gt 0 ]]; then
                 log "Warmup: transport=mqtt broker=${bname} payload=${WARMUP_PAYLOAD}B duration=${WARMUP_SECS}s"
                 local _orig_dur="${DURATION}"
                 DURATION="${WARMUP_SECS}"
                 SKIP_SUMMARY=1
                 run_single_execution "mqtt" "${WARMUP_PAYLOAD}" "${PAIRS}" "${TOTAL_RATE}" "${bname}" "${bhost}" "${bport}"
                 SKIP_SUMMARY=0
                 DURATION="${_orig_dur}"
               fi
            fi

            log "Run: transport=mqtt broker=${bname} payload=${p}B pairs=${PAIRS} total_rate=${TOTAL_RATE}/s"
            run_single_execution "mqtt" "${p}" "${PAIRS}" "${TOTAL_RATE}" "${bname}" "${bhost}" "${bport}"
            
            # Interval sleep (cooldown) - only if not sequential (restart handles settling)
            if [[ $SEQUENTIAL -eq 0 ]] && [[ ${INTERVAL_SEC} -gt 0 ]]; then
              log "Cooldown: sleeping ${INTERVAL_SEC}s..."
              if [[ ${DRY_RUN} -eq 1 ]]; then echo "+ sleep ${INTERVAL_SEC}"; else sleep ${INTERVAL_SEC}; fi
            fi
          done
          
          # Stop service if sequential (after all payloads for this broker)
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

      for p in "${PAYLOADS[@]}"; do
        # Start or restart service before each iteration
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
           
           # Warmup after broker restart (skip if WARMUP_SECS=0)
           if [[ ${WARMUP_SECS} -gt 0 ]]; then
             log "Warmup: transport=${t} payload=${WARMUP_PAYLOAD}B duration=${WARMUP_SECS}s"
             local _orig_dur="${DURATION}"
             DURATION="${WARMUP_SECS}"
             SKIP_SUMMARY=1
             run_single_execution "${t}" "${WARMUP_PAYLOAD}" "${PAIRS}" "${TOTAL_RATE}"
             SKIP_SUMMARY=0
             DURATION="${_orig_dur}"
           fi
        fi

        log "Run: transport=${t} payload=${p}B pairs=${PAIRS} total_rate=${TOTAL_RATE}/s"
        run_single_execution "${t}" "${p}" "${PAIRS}" "${TOTAL_RATE}"
        
        # Interval sleep (cooldown) - only if not sequential (restart handles settling)
        if [[ $SEQUENTIAL -eq 0 ]] && [[ ${INTERVAL_SEC} -gt 0 ]]; then
          log "Cooldown: sleeping ${INTERVAL_SEC}s..."
          if [[ ${DRY_RUN} -eq 1 ]]; then echo "+ sleep ${INTERVAL_SEC}"; else sleep ${INTERVAL_SEC}; fi
        fi
      done
      
      # Stop service if sequential (after all payloads for this transport)
      if [[ $SEQUENTIAL -eq 1 ]] && [[ -n "$svc" ]]; then
         manage_service down "$svc"
      fi
    fi
  done

  log "Plotting results to ${PLOTS_DIR}"
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ python3 scripts/plot_results.py --summary ${SUMMARY_CSV} --out-dir ${PLOTS_DIR} --only-latency-vs-payload"
  else
    python3 "${SCRIPT_DIR}/plot_results.py" --summary "${SUMMARY_CSV}" --out-dir "${PLOTS_DIR}" --only-latency-vs-payload
  fi

  log "Done. Summary CSV: ${SUMMARY_CSV}"
}

main "$@"