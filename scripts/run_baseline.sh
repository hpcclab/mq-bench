#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Baseline scenario: 1 publisher → 1 subscriber (aggregated CSV)
# Supports ENGINE=zenoh|mqtt|redis via env variables used by lib.sh
# Usage: scripts/run_baseline.sh [RUN_ID] [RATE=10000] [DURATION=20]

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
def RATE     "${RATE:-10000}"
def DURATION "${DURATION:-20}"
def PAYLOAD  "${PAYLOAD:-1024}"
def SNAPSHOT "${SNAPSHOT:-5}"
ENGINE="${ENGINE:-zenoh}"
KEY="${KEY:-bench/topic}"

ART_DIR="artifacts/${RUN_ID}/local_baseline"
BIN="./target/release/mq-bench"

echo "[run_baseline] Run ID: ${RUN_ID} | ENGINE=${ENGINE} | RATE=${RATE} DURATION=${DURATION}s"
mkdir -p "${ART_DIR}"
build_release_if_needed "${BIN}"

SUB_CSV="${ART_DIR}/sub.csv"
PUB_CSV="${ART_DIR}/pub.csv"
STATS_CSV="${ART_DIR}/docker_stats.csv"

# Determine containers to monitor (can be overridden via MONITOR_CONTAINERS or BROKER_CONTAINER)
declare -a MON_CONTAINERS=()
resolve_monitor_containers MON_CONTAINERS
if (( ${#MON_CONTAINERS[@]} > 0 )); then
	echo "[monitor] Capturing docker stats for: ${MON_CONTAINERS[*]} → ${STATS_CSV}"
	start_broker_stats_monitor STATS_PID "${STATS_CSV}" "${MON_CONTAINERS[@]}"
	trap 'echo "Stopping subscriber (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true; stop_broker_stats_monitor ${STATS_PID}' EXIT
else
	trap 'echo "Stopping subscriber (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT
fi

echo "Starting subscriber → ${KEY}"
start_sub SUB_PID "${KEY}" 1 "${SUB_CSV}" "${ART_DIR}/sub.log"

sleep 1

echo "Running publisher → ${KEY}"
start_pub PUB_PID "${KEY}" "${PAYLOAD}" "${RATE}" "${DURATION}" "${PUB_CSV}" "${ART_DIR}/pub.log"

watch_until_pub_exits ${PUB_PID} "${SUB_CSV}" "${PUB_CSV}"
wait ${PUB_PID} || true

echo "=== Summary (${RUN_ID}) ==="
summarize_common "${SUB_CSV}" "${PUB_CSV}"

echo "Baseline run complete. Artifacts at ${ART_DIR}"
