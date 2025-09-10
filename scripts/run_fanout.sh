#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Fanout scenario: 1 publisher → N subscribers.
# Now supports multiple transports via ENGINE env var: zenoh|mqtt|redis|nats
# Usage: scripts/run_fanout.sh [RUN_ID] [SUBS=4] [RATE=10000] [DURATION=30]
# Env:
#   ENGINE=zenoh (default) | mqtt | redis | nats
#   For zenoh:   ENDPOINT_SUB=tcp/127.0.0.1:7448  ENDPOINT_PUB=tcp/127.0.0.1:7447  [optional] ZENOH_MODE=
#   For mqtt:    MQTT_HOST=127.0.0.1  MQTT_PORT=1883
#   For redis:   REDIS_URL=redis://127.0.0.1:6379
#   For nats:    NATS_HOST=127.0.0.1  NATS_PORT=4222

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
SUBS=${2:-${SUBS:-4}}
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

echo "[run_fanout] Run ID: ${RUN_ID} | ENGINE=${ENGINE} | SUBS=${SUBS} RATE=${RATE} DURATION=${DURATION}s"
mkdir -p "${ART_DIR}"

build_release_if_needed "${BIN}"

SUB_CSV="${ART_DIR}/sub_agg.csv"
PUB_CSV="${ART_DIR}/pub.csv"
STATS_CSV="${ART_DIR}/docker_stats.csv"

# Determine containers to monitor
declare -a MON_CONTAINERS=()
resolve_monitor_containers MON_CONTAINERS
if (( ${#MON_CONTAINERS[@]} > 0 )); then
	echo "[monitor] Capturing docker stats for: ${MON_CONTAINERS[*]} → ${STATS_CSV}"
	start_broker_stats_monitor STATS_PID "${STATS_CSV}" "${MON_CONTAINERS[@]}"
	trap 'echo "Stopping subscribers (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true; stop_broker_stats_monitor ${STATS_PID}' EXIT
else
	trap 'echo "Stopping subscribers (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT
fi

echo "Starting ${SUBS} subscribers → ${KEY} (aggregated CSV)"
start_sub SUB_PID "${KEY}" "${SUBS}" "${SUB_CSV}" "${ART_DIR}/sub.log"

sleep 1

echo "Running publisher → ${KEY}"
start_pub PUB_PID "${KEY}" "${PAYLOAD}" "${RATE}" "${DURATION}" "${PUB_CSV}" "${ART_DIR}/pub.log"

print_status() {
	local sub_file="$1" pub_file="$2"
	local last_sub last_pub
	last_sub=$(tail -n +2 "$sub_file" 2>/dev/null | tail -n1 || true)
	last_pub=$(tail -n +2 "$pub_file" 2>/dev/null | tail -n1 || true)
	local spub itpub ttpub rsub itsub p99sub
	if [[ -n "$last_pub" ]]; then
		IFS=, read -r _ spub _ epub ttpub itpub _ _ _ _ _ _ <<<"$last_pub"
	fi
	if [[ -n "$last_sub" ]]; then
		IFS=, read -r _ _ rsub _ _ itsub _ _ p99sub _ _ _ <<<"$last_sub"
	fi
	printf "[status] PUB sent=%s itps=%s tps=%s | SUB recv=%s itps=%s p99=%.2fms\n" \
		"${spub:--}" "${itpub:--}" "${ttpub:--}" \
		"${rsub:--}" "${itsub:--}" "$(awk -v n="${p99sub:-0}" 'BEGIN{printf (n/1e6)}')"
}

watch_until_pub_exits ${PUB_PID} "${SUB_CSV}" "${PUB_CSV}"

wait ${PUB_PID} || true

echo "\n=== Summary (${RUN_ID}) ==="
summarize_common "${SUB_CSV}" "${PUB_CSV}"

echo "Fanout run complete. Artifacts at ${ART_DIR}"