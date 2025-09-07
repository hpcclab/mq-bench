#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
def PAYLOAD "${PAYLOAD:-1024}"
def RATE     "${RATE:-10000}"
def DURATION "${DURATION:-20}"
def SNAPSHOT "${SNAPSHOT:-5}"
MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1884}"  # EMQX mapped port
KEY="${KEY:-bench/topic}"

ART_DIR="artifacts/${RUN_ID}/mqtt_emqx_baseline"
BIN="./target/release/mq-bench"

echo "[run_mqtt_baseline_emqx] Run ID: ${RUN_ID} (host=${MQTT_HOST} port=${MQTT_PORT})"
mkdir -p "${ART_DIR}"

echo "Building release binary with mqtt feature..."
cargo build --release --features transport-mqtt

SUB_CSV="${ART_DIR}/sub.csv"
PUB_CSV="${ART_DIR}/pub.csv"

echo "Starting MQTT subscriber (EMQX) → ${KEY}"
"${BIN}" --snapshot-interval "${SNAPSHOT}" sub \
  --engine mqtt \
  --connect "host=${MQTT_HOST}" \
  --connect "port=${MQTT_PORT}" \
  --expr "${KEY}" \
  --csv "${SUB_CSV}" \
  >"${ART_DIR}/sub.log" 2>&1 &
SUB_PID=$!
trap 'echo "Stopping subscriber (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running MQTT publisher (EMQX) → ${KEY} (payload=${PAYLOAD}, rate=${RATE}, duration=${DURATION}s, snap=${SNAPSHOT}s)"
RATE_FLAG=()
if [[ -n "${RATE}" ]] && (( RATE > 0 )); then RATE_FLAG=(--rate "${RATE}"); fi
"${BIN}" --snapshot-interval "${SNAPSHOT}" pub \
  --engine mqtt \
  --connect "host=${MQTT_HOST}" \
  --connect "port=${MQTT_PORT}" \
  --topic-prefix "${KEY}" \
  --payload "${PAYLOAD}" \
  "${RATE_FLAG[@]}" \
  --duration "${DURATION}" \
  --csv "${PUB_CSV}" \
  >"${ART_DIR}/pub.log" 2>&1 &
PUB_PID=$!

echo "[watch] printing status every ${SNAPSHOT}s..."
while kill -0 ${PUB_PID} 2>/dev/null; do
  sleep "${SNAPSHOT}"
  last_pub=$(tail -n +2 "${PUB_CSV}" 2>/dev/null | tail -n1 || true)
  last_sub=$(tail -n +2 "${SUB_CSV}" 2>/dev/null | tail -n1 || true)
  if [[ -n "$last_pub" ]]; then IFS=, read -r _ spub _ _ ttpub itpub _ _ _ _ _ _ <<<"$last_pub"; fi
  if [[ -n "$last_sub" ]]; then IFS=, read -r _ _ rsub _ _ itsub _ _ p99sub _ _ _ <<<"$last_sub"; fi
  printf "[status] PUB sent=%s itps=%s tps=%s | SUB recv=%s itps=%s p99=%.2fms\n" \
    "${spub:--}" "${itpub:--}" "${ttpub:--}" \
    "${rsub:--}" "${itsub:--}" "$(awk -v n="${p99sub:-0}" 'BEGIN{printf (n/1e6)}')"
done

wait ${PUB_PID} || true

echo "Run complete. Artifacts at ${ART_DIR}"
