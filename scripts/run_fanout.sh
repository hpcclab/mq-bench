#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Fanout scenario: M publishers → N subscribers.
# Now supports multiple transports via ENGINE env var: zenoh|mqtt|redis|nats
# Usage: scripts/run_fanout.sh [RUN_ID] [SUBS=4] [RATE=10000] [DURATION=30]
# Env:
#   ENGINE=zenoh (default) | mqtt | redis | nats
#   PUBS=1              Number of publishers (default: 1)
#   SUBS=4              Number of subscribers (default: 4)
#   For zenoh:   ENDPOINT_SUB=tcp/127.0.0.1:7447  ENDPOINT_PUB=tcp/127.0.0.1:7447  [optional] ZENOH_MODE=
#   For mqtt:    MQTT_HOST=127.0.0.1  MQTT_PORT=1883
#   For redis:   REDIS_URL=redis://127.0.0.1:6379
#   For nats:    NATS_HOST=127.0.0.1  NATS_PORT=4222

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
SUBS=${2:-${SUBS:-4}}
def PUBS     "${PUBS:-1}"
def RATE     "${RATE:-10000}"
def DURATION "${DURATION:-30}"
def PAYLOAD  "${PAYLOAD:-1024}"
def SNAPSHOT "${SNAPSHOT:-5}"
ENGINE="${ENGINE:-zenoh}"

ART_DIR="artifacts/${RUN_ID}/fanout_singlesite"
BIN="./target/release/mq-bench"
ENDPOINT_PUB="${ENDPOINT_PUB:-tcp/127.0.0.1:7447}"
ENDPOINT_SUB="${ENDPOINT_SUB:-tcp/127.0.0.1:7447}"
MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
KEY="${KEY:-bench/topic}"
ZENOH_MODE="${ZENOH_MODE:-}"

echo "[run_fanout] Run ID: ${RUN_ID} | ENGINE=${ENGINE} | PUBS=${PUBS} SUBS=${SUBS} RATE=${RATE} DURATION=${DURATION}s"
mkdir -p "${ART_DIR}"

build_release_if_needed "${BIN}"

SUB_CSV="${ART_DIR}/sub_agg.csv"
PUB_CSV="${ART_DIR}/pub_agg.csv"
STATS_CSV="${ART_DIR}/docker_stats.csv"

# Arrays for tracking publisher PIDs and CSVs
declare -a PUB_PIDS=()
declare -a PUB_CSVS=()
declare -a PUB_LOGS=()

# Determine containers to monitor
declare -a MON_CONTAINERS=()
resolve_monitor_containers MON_CONTAINERS
if (( ${#MON_CONTAINERS[@]} > 0 )); then
	echo "[monitor] Capturing docker stats for: ${MON_CONTAINERS[*]} → ${STATS_CSV}"
	start_broker_stats_monitor STATS_PID "${STATS_CSV}" "${MON_CONTAINERS[@]}"
	trap 'echo "Stopping subscribers (${SUB_PID}) and publishers (${PUB_PIDS[*]})"; kill ${SUB_PID} "${PUB_PIDS[@]}" >/dev/null 2>&1 || true; stop_broker_stats_monitor ${STATS_PID}' EXIT
else
	trap 'echo "Stopping subscribers (${SUB_PID}) and publishers (${PUB_PIDS[*]})"; kill ${SUB_PID} "${PUB_PIDS[@]}" >/dev/null 2>&1 || true' EXIT
fi

echo "Starting ${SUBS} subscribers → ${KEY} (aggregated CSV)"
start_sub SUB_PID "${KEY}" "${SUBS}" "${SUB_CSV}" "${ART_DIR}/sub.log"

sleep 1

# Calculate per-publisher rate
if (( PUBS > 1 )); then
	PUB_RATE=$(( RATE / PUBS ))
	echo "Running ${PUBS} publishers → ${KEY} (total_rate=${RATE}, per_pub_rate=${PUB_RATE})"
else
	PUB_RATE=${RATE}
	echo "Running publisher → ${KEY}"
fi

# Start multiple publishers
for (( p=0; p<PUBS; p++ )); do
	PUB_CSVS+=("${ART_DIR}/pub_${p}.csv")
	PUB_LOGS+=("${ART_DIR}/pub_${p}.log")
	local_pid=0
	start_pub local_pid "${KEY}" "${PAYLOAD}" "${PUB_RATE}" "${DURATION}" "${PUB_CSVS[$p]}" "${PUB_LOGS[$p]}"
	PUB_PIDS+=("${local_pid}")
	# Small stagger to avoid thundering herd on broker
	if (( PUBS > 1 && p < PUBS - 1 )); then
		sleep 0.1
	fi
done

print_status() {
	local sub_file="$1"
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

# Aggregate publisher CSVs
aggregate_pub_csvs "${PUB_CSV}" "${PUB_CSVS[@]}"

echo -e "\n=== Summary (${RUN_ID}) ==="
echo "Publishers: ${PUBS} | Subscribers: ${SUBS}"
summarize_common "${SUB_CSV}" "${PUB_CSV}"

echo "Fanout run complete. Artifacts at ${ART_DIR}"