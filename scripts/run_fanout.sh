#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

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

# Fanout scenario: M publishers → N subscribers.
# Now supports multiple transports via ENGINE env var: zenoh|mqtt|redis|nats
# Usage: scripts/run_fanout.sh [RUN_ID] [SUBS=4] [RATE=10000] [DURATION=30]
# Env:
#   ENGINE=zenoh (default) | mqtt | redis | nats
#   PUBS=1              Number of publishers (default: 1)
#   SUBS=4              Number of subscribers (default: 4)
#   SUB_PROCS_PER_TOPIC=1
#                       Controlled fanout only: split each topic's subscribers
#                       across this many client processes to avoid client-side bottlenecks.
#   SUB_RAMP_UP_SECS=0  Spread subscriber connection creation over this many seconds.
#                       If unset, defaults to 60s when SUBS >= 2000.
#   RATE_PROFILE=...    Optional per-publisher phases name:duration:rate, overrides RATE per publisher
#   For zenoh:   ENDPOINT_SUB=tcp/127.0.0.1:7447  ENDPOINT_PUB=tcp/127.0.0.1:7447  [optional] ZENOH_MODE=
#   For mqtt:    MQTT_HOST=127.0.0.1  MQTT_PORT=1883
#   For redis:   REDIS_URL=redis://127.0.0.1:6379
#   For nats:    NATS_HOST=127.0.0.1  NATS_PORT=4222

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
SUBS=${2:-${SUBS:-4}}
DURATION_WAS_SET=0
if [[ -n "${DURATION:-}" ]]; then DURATION_WAS_SET=1; fi
def PUBS     "${PUBS:-1}"
def RATE     "${RATE:-10000}"
def DURATION "${DURATION:-30}"
def PAYLOAD  "${PAYLOAD:-1024}"
def SNAPSHOT "${SNAPSHOT:-5}"
RATE_PROFILE="${RATE_PROFILE:-}"
SUB_RAMP_UP_SECS="${SUB_RAMP_UP_SECS:-}"
if [[ -z "${SUB_RAMP_UP_SECS}" ]]; then
	if (( SUBS >= 2000 )); then
		SUB_RAMP_UP_SECS=60
	else
		SUB_RAMP_UP_SECS=0
	fi
fi
if [[ -n "${RATE_PROFILE}" && ${DURATION_WAS_SET} -eq 0 ]]; then
	if ! DURATION="$(profile_duration_secs "${RATE_PROFILE}")"; then
		echo "[run_fanout] Invalid RATE_PROFILE: ${RATE_PROFILE}" >&2
		exit 2
	fi
fi
ENGINE="${ENGINE:-zenoh}"

ART_DIR="artifacts/${RUN_ID}/fanout_singlesite"
BIN="./target/release/mq-bench"
ENDPOINT_PUB="${ENDPOINT_PUB:-tcp/127.0.0.1:7447}"
ENDPOINT_SUB="${ENDPOINT_SUB:-tcp/127.0.0.1:7447}"
MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
KEY="${KEY:-bench/topic}"
CONTROLLED_FANOUT="${CONTROLLED_FANOUT:-0}"
SUBS_PER_PUB="${SUBS_PER_PUB:-${SUBS}}"
SUB_PROCS_PER_TOPIC="${SUB_PROCS_PER_TOPIC:-1}"
TOPIC_PREFIX="${TOPIC_PREFIX:-topic}"
ZENOH_MODE="${ZENOH_MODE:-}"

if [[ -n "${RATE_PROFILE}" ]]; then
	echo "[run_fanout] Run ID: ${RUN_ID} | ENGINE=${ENGINE} | PUBS=${PUBS} SUBS=${SUBS} SUB_RAMP_UP_SECS=${SUB_RAMP_UP_SECS} RATE_PROFILE=${RATE_PROFILE} DURATION=${DURATION}s"
else
	echo "[run_fanout] Run ID: ${RUN_ID} | ENGINE=${ENGINE} | PUBS=${PUBS} SUBS=${SUBS} SUB_RAMP_UP_SECS=${SUB_RAMP_UP_SECS} RATE=${RATE} DURATION=${DURATION}s"
fi
mkdir -p "${ART_DIR}"

if [[ "${CONTROLLED_FANOUT}" == "1" ]]; then
	if [[ ! "${SUBS_PER_PUB}" =~ ^[0-9]+$ ]] || (( SUBS_PER_PUB <= 0 )); then
		echo "[run_fanout] Invalid SUBS_PER_PUB=${SUBS_PER_PUB}" >&2
		exit 2
	fi
	if (( PUBS * SUBS_PER_PUB != SUBS )); then
		echo "[run_fanout] ERROR: controlled fan-out requires PUBS * SUBS_PER_PUB == SUBS; got PUBS=${PUBS}, SUBS_PER_PUB=${SUBS_PER_PUB}, SUBS=${SUBS}" >&2
		exit 2
	fi
	if [[ ! "${SUB_PROCS_PER_TOPIC}" =~ ^[0-9]+$ ]] || (( SUB_PROCS_PER_TOPIC <= 0 )); then
		echo "[run_fanout] Invalid SUB_PROCS_PER_TOPIC=${SUB_PROCS_PER_TOPIC}" >&2
		exit 2
	fi
fi

build_release_if_needed "${BIN}"

SUB_CSV="${ART_DIR}/sub_agg.csv"
PUB_CSV="${ART_DIR}/pub_agg.csv"
STATS_CSV="${ART_DIR}/docker_stats.csv"

# Arrays for tracking subscriber and publisher PIDs/CSVs.
declare -a SUB_PIDS=()
declare -a SUB_CSVS=()
declare -a SUB_LOGS=()
declare -a PUB_PIDS=()
declare -a PUB_CSVS=()
declare -a PUB_LOGS=()

topic_for_group() {
	local group_id="$1"
	echo "${TOPIC_PREFIX}_${group_id}"
}

cleanup_run_clients() {
	echo "Stopping subscribers (${SUB_PIDS[*]:-}) and publishers (${PUB_PIDS[*]:-})"
	local pids=("${SUB_PIDS[@]}" "${PUB_PIDS[@]}")
	if (( ${#pids[@]} > 0 )); then
		kill "${pids[@]}" >/dev/null 2>&1 || true
	fi
	if (( ${STATS_PID:-0} > 0 )); then
		stop_broker_stats_monitor "${STATS_PID}"
	fi
}

# Determine containers to monitor
declare -a MON_CONTAINERS=()
STATS_PID=0
resolve_monitor_containers MON_CONTAINERS
if (( ${#MON_CONTAINERS[@]} > 0 )); then
	echo "[monitor] Capturing docker stats for: ${MON_CONTAINERS[*]} → ${STATS_CSV}"
	start_broker_stats_monitor STATS_PID "${STATS_CSV}" "${MON_CONTAINERS[@]}"
fi
trap cleanup_run_clients EXIT

if [[ "${CONTROLLED_FANOUT}" == "1" ]]; then
	echo "Starting ${SUBS} subscribers across ${PUBS} controlled fan-out topics (${SUBS_PER_PUB} per topic, ${SUB_PROCS_PER_TOPIC} client process(es) per topic)"
	# Controlled fan-out uses multiple topics, each with a fixed subscriber group.
	# Broadcast fan-out would use one shared topic and deliver every message to all subscribers.
	for ((group_id = 0; group_id < PUBS; group_id++)); do
		topic="$(topic_for_group "${group_id}")"
		for ((shard_id = 0; shard_id < SUB_PROCS_PER_TOPIC; shard_id++)); do
			shard_subs=$(( SUBS_PER_PUB / SUB_PROCS_PER_TOPIC ))
			if (( shard_id < SUBS_PER_PUB % SUB_PROCS_PER_TOPIC )); then
				shard_subs=$(( shard_subs + 1 ))
			fi
			if (( shard_subs <= 0 )); then
				continue
			fi
			if (( SUB_PROCS_PER_TOPIC == 1 )); then
				sub_csv="${ART_DIR}/sub_${group_id}.csv"
				sub_log="${ART_DIR}/sub_${group_id}.log"
			else
				sub_csv="${ART_DIR}/sub_${group_id}_${shard_id}.csv"
				sub_log="${ART_DIR}/sub_${group_id}_${shard_id}.log"
			fi
			SUB_CSVS+=("${sub_csv}")
			SUB_LOGS+=("${sub_log}")
			sub_pid=0
			start_sub sub_pid "${topic}" "${shard_subs}" "${sub_csv}" "${sub_log}"
			SUB_PIDS+=("${sub_pid}")
		done
	done
else
	echo "Starting ${SUBS} subscribers → ${KEY} (aggregated CSV)"
	SUB_CSVS+=("${SUB_CSV}")
	SUB_LOGS+=("${ART_DIR}/sub.log")
	sub_pid=0
	start_sub sub_pid "${KEY}" "${SUBS}" "${SUB_CSV}" "${ART_DIR}/sub.log"
	SUB_PIDS+=("${sub_pid}")
fi

if [[ "${SUB_RAMP_UP_SECS}" != "0" && "${SUB_RAMP_UP_SECS}" != "0.0" ]]; then
	echo "[sub] Waiting ${SUB_RAMP_UP_SECS}s for subscriber ramp-up before starting publishers"
	sleep "${SUB_RAMP_UP_SECS}"
fi
sleep 1

# Calculate per-publisher rate for steady mode. RATE_PROFILE already uses per-publisher rates.
if [[ -n "${RATE_PROFILE}" ]]; then
	PUB_RATE=0
	rate_desc="profile=${RATE_PROFILE}"
elif (( PUBS > 1 )); then
	PUB_RATE=$(( RATE / PUBS ))
	rate_desc="total_rate=${RATE}, per_pub_rate=${PUB_RATE}"
else
	PUB_RATE=${RATE}
	rate_desc="rate=${PUB_RATE}"
fi

if [[ "${CONTROLLED_FANOUT}" == "1" ]]; then
	echo "Running ${PUBS} controlled publishers across ${PUBS} topics (${rate_desc})"
	for ((group_id = 0; group_id < PUBS; group_id++)); do
		topic="$(topic_for_group "${group_id}")"
		PUB_CSVS+=("${ART_DIR}/pub_${group_id}.csv")
		PUB_LOGS+=("${ART_DIR}/pub_${group_id}.log")
		pub_pid=0
		start_pub pub_pid "${topic}" "${PAYLOAD}" "${PUB_RATE}" "${DURATION}" "${PUB_CSVS[group_id]}" "${PUB_LOGS[group_id]}" 1
		PUB_PIDS+=("${pub_pid}")
	done
else
	echo "Running ${PUBS} publishers → ${KEY} (${rate_desc})"
	# Start one aggregate publisher process. The mq-bench multi-publisher path assigns
	# interleaved sequence ranges, avoiding duplicate sequence IDs across publishers.
	PUB_CSVS+=("${ART_DIR}/pub_0.csv")
	PUB_LOGS+=("${ART_DIR}/pub_0.log")
	pub_pid=0
	start_pub pub_pid "${KEY}" "${PAYLOAD}" "${PUB_RATE}" "${DURATION}" "${PUB_CSVS[0]}" "${PUB_LOGS[0]}" "${PUBS}"
	PUB_PIDS+=("${pub_pid}")
fi

aggregate_sub_csvs() {
	local out_csv="$1"; shift
	local csvs=("$@")
	if (( ${#csvs[@]} == 0 )); then return 0; fi
	python3 - "${out_csv}" "${csvs[@]}" <<'PY'
import csv
import os
import sys
from collections import defaultdict

out = sys.argv[1]
paths = sys.argv[2:]
fieldnames = None
series = []
current_by_ts = defaultdict(list)

for path in paths:
    if not os.path.exists(path):
        continue
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        if fieldnames is None and reader.fieldnames:
            fieldnames = list(reader.fieldnames)
        rows = []
        for row in reader:
            try:
                ts = int(float(row.get("timestamp") or 0))
            except ValueError:
                continue
            if ts <= 0:
                continue
            row["_ts"] = ts
            rows.append(row)
            current_by_ts[ts].append(row)
        if rows:
            rows.sort(key=lambda r: r["_ts"])
            series.append(rows)

if fieldnames is None:
    fieldnames = "timestamp,sent_count,received_count,error_count,total_throughput,interval_throughput,latency_ns_p25,latency_ns_p50,latency_ns_p75,latency_ns_p95,latency_ns_p99,latency_ns_min,latency_ns_max,latency_ns_mean,latency_ns_stddev,latency_sample_count,connections,active_connections,connection_attempts,connection_failures,crashes_injected,reconnects,reconnect_failures,duplicate_count,gap_count,interval_latency_ns_p50,interval_latency_ns_p95,interval_latency_ns_p99,interval_latency_ns_mean,interval_latency_sample_count".split(",")

os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
timestamps = sorted(current_by_ts)

def num(row, key):
    try:
        raw = row.get(key, "")
        return float(raw) if raw not in (None, "") else 0.0
    except ValueError:
        return 0.0

def fmt(value):
    if abs(value - round(value)) < 1e-6:
        return str(int(round(value)))
    return f"{value:.2f}"

def weighted(rows, value_key, weight_key):
    total_weight = sum(num(r, weight_key) for r in rows)
    if total_weight <= 0:
        return 0.0
    return sum(num(r, value_key) * num(r, weight_key) for r in rows) / total_weight

with open(out, "w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fieldnames)
    writer.writeheader()
    if not timestamps:
        raise SystemExit(0)

    idxs = [-1] * len(series)
    for ts in timestamps:
        latest = []
        for idx, rows in enumerate(series):
            while idxs[idx] + 1 < len(rows) and rows[idxs[idx] + 1]["_ts"] <= ts:
                idxs[idx] += 1
            if idxs[idx] >= 0:
                latest.append(rows[idxs[idx]])
        current = current_by_ts.get(ts, [])

        out_row = {name: "0" for name in fieldnames}
        out_row["timestamp"] = str(ts)

        sum_latest = [
            "sent_count", "received_count", "error_count", "latency_sample_count",
            "connections", "active_connections", "connection_attempts", "connection_failures",
            "crashes_injected", "reconnects", "reconnect_failures", "duplicate_count", "gap_count",
            "total_throughput",
        ]
        sum_current = ["interval_throughput", "interval_latency_sample_count"]
        for key in sum_latest:
            if key in out_row:
                out_row[key] = fmt(sum(num(r, key) for r in latest))
        for key in sum_current:
            if key in out_row:
                out_row[key] = fmt(sum(num(r, key) for r in current))

        for key in ["latency_ns_p25", "latency_ns_p50", "latency_ns_p75", "latency_ns_p95", "latency_ns_p99", "latency_ns_mean", "latency_ns_stddev"]:
            if key in out_row:
                out_row[key] = fmt(weighted(latest, key, "latency_sample_count"))
        if "latency_ns_min" in out_row:
            vals = [num(r, "latency_ns_min") for r in latest if num(r, "latency_ns_min") > 0]
            out_row["latency_ns_min"] = fmt(min(vals)) if vals else "0"
        if "latency_ns_max" in out_row:
            vals = [num(r, "latency_ns_max") for r in latest if num(r, "latency_ns_max") > 0]
            out_row["latency_ns_max"] = fmt(max(vals)) if vals else "0"

        for key in ["interval_latency_ns_p50", "interval_latency_ns_p95", "interval_latency_ns_p99", "interval_latency_ns_mean"]:
            if key in out_row:
                out_row[key] = fmt(weighted(current, key, "interval_latency_sample_count"))

        writer.writerow(out_row)
PY
}

print_status() {
	local sub_file="$1"
	if (( ${#SUB_CSVS[@]} > 1 )); then
		aggregate_sub_csvs "${sub_file}" "${SUB_CSVS[@]}" || true
	fi
	local last_sub
	last_sub=$(tail -n +2 "$sub_file" 2>/dev/null | tail -n1 || true)
	local rsub itsub p99sub conns_sub active_sub
	# Aggregate publisher stats from all pub CSVs
	local total_sent=0 total_tps=0
	for pc in "${PUB_CSVS[@]}"; do
		local last_pub
		last_pub=$(tail -n +2 "$pc" 2>/dev/null | tail -n1 || true)
		if [[ -n "$last_pub" ]]; then
			local spub ttpub
			IFS=, read -r _ spub _ _ ttpub _ <<<"$last_pub"
			total_sent=$((total_sent + ${spub:-0}))
			total_tps=$(awk "BEGIN{print ${total_tps} + ${ttpub:-0}}")
		fi
	done
	if [[ -n "$last_sub" ]]; then
		IFS=, read -r _ _ rsub _ _ itsub _ _ p99sub _ _ _ conns_sub active_sub <<<"$last_sub"
	fi
	printf "[status] PUB sent=%s tps=%.0f (pubs=%d) | SUB recv=%s itps=%s p99=%.2fms conn=%s/%s\n" \
		"${total_sent}" "${total_tps}" "${PUBS}" \
		"${rsub:--}" "${itsub:--}" "$(awk -v n="${p99sub:-0}" 'BEGIN{printf (n/1e6)}')" \
		"${conns_sub:--}" "${active_sub:--}"
}

# Aggregate publisher CSVs for unified metrics
aggregate_pub_csvs() {
	local out_csv="$1"; shift
	local csvs=("$@")
	# Write header from first CSV
	if [[ -f "${csvs[0]}" ]]; then
		head -1 "${csvs[0]}" > "${out_csv}"
	fi
	# For simplicity, take last row from each and sum key metrics
	# This gives an aggregate snapshot
	local total_sent=0 total_err=0 total_tps=0
	for pc in "${csvs[@]}"; do
		if [[ -f "$pc" ]]; then
			local last_row
			last_row=$(tail -n +2 "$pc" | tail -1 || true)
			if [[ -n "$last_row" ]]; then
				IFS=, read -r ts sent pub_recv err tps itps p50 p95 p99 jit min max conns active <<<"$last_row"
				total_sent=$((total_sent + ${sent:-0}))
				total_err=$((total_err + ${err:-0}))
				total_tps=$(awk "BEGIN{print ${total_tps} + ${tps:-0}}")
			fi
		fi
	done
	# Write aggregate row (use last timestamp from last CSV)
	local ts
	ts=$(tail -n +2 "${csvs[-1]}" 2>/dev/null | tail -1 | cut -d, -f1 || date +%s)
	echo "${ts},${total_sent},0,${total_err},${total_tps},${total_tps},0,0,0,0,0,0,${PUBS},${PUBS}" >> "${out_csv}"
}

# Wait for all publishers to exit
wait_for_publishers() {
	echo "[watch] printing status every ${SNAPSHOT}s..."
	while true; do
		local any_running=0
		for pid in "${PUB_PIDS[@]}"; do
			if kill -0 "${pid}" 2>/dev/null; then
				any_running=1
				break
			fi
		done
		if (( any_running == 0 )); then
			break
		fi
		print_status "${SUB_CSV}"
		sleep "${SNAPSHOT}"
	done
}

wait_for_publishers

# Wait for all publisher processes
for pid in "${PUB_PIDS[@]}"; do
	wait "${pid}" || true
done

# Aggregate subscriber and publisher CSVs
if (( ${#SUB_CSVS[@]} > 1 )); then
	aggregate_sub_csvs "${SUB_CSV}" "${SUB_CSVS[@]}"
fi
aggregate_pub_csvs "${PUB_CSV}" "${PUB_CSVS[@]}"

echo -e "\n=== Summary (${RUN_ID}) ==="
echo "Publishers: ${PUBS} | Subscribers: ${SUBS}"
summarize_common "${SUB_CSV}" "${PUB_CSV}"

echo "Fanout run complete. Artifacts at ${ART_DIR}"