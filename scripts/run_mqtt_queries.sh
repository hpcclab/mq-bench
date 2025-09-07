#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# MQTT requester/queryable scenario
# Usage: scripts/run_mqtt_queries.sh [RUN_ID]

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
QPS=${QPS:-2000}
CONC=${CONC:-32}
DURATION=${DURATION:-20}
TIMEOUT_MS=${TIMEOUT_MS:-2000}
REPLY_SIZE=${REPLY_SIZE:-1024}
SNAPSHOT=${SNAPSHOT:-5}
MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"
PREFIX="${PREFIX:-bench/qry}"
KEY_EXPR="${PREFIX}/item"

ART_DIR="artifacts/${RUN_ID}/mqtt_queries"
BIN="./target/release/mq-bench"

echo "[run_mqtt_queries] Run ID: ${RUN_ID} | QPS=${QPS} CONC=${CONC} TIMEOUT=${TIMEOUT_MS}ms DURATION=${DURATION}s"
mkdir -p "${ART_DIR}"

echo "Building release binary with mqtt feature..."
cargo build --release --features transport-mqtt

QRY_CSV="${ART_DIR}/queryable.csv"
REQ_CSV="${ART_DIR}/requester.csv"

echo "Starting MQTT queryable serving ${PREFIX}/item"
"${BIN}" --snapshot-interval "${SNAPSHOT}" qry \
  --engine mqtt \
  --connect "host=${MQTT_HOST}" \
  --connect "port=${MQTT_PORT}" \
  --serve-prefix "${KEY_EXPR}" \
  --reply-size "${REPLY_SIZE}" \
  --csv "${QRY_CSV}" \
  >"${ART_DIR}/qry.log" 2>&1 &
QRY_PID=$!
trap 'echo "Stopping queryable (${QRY_PID})"; kill ${QRY_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running MQTT requester for ${KEY_EXPR}"
QPS_FLAG=()
if [[ -n "${QPS}" ]] && (( QPS > 0 )); then QPS_FLAG=(--qps "${QPS}"); fi
"${BIN}" --snapshot-interval "${SNAPSHOT}" req \
  --engine mqtt \
  --connect "host=${MQTT_HOST}" \
  --connect "port=${MQTT_PORT}" \
  --key-expr "${KEY_EXPR}" \
  "${QPS_FLAG[@]}" \
  --concurrency "${CONC}" \
  --timeout "${TIMEOUT_MS}" \
  --duration "${DURATION}" \
  --csv "${REQ_CSV}" \
  >"${ART_DIR}/req.log" 2>&1 &
REQ_PID=$!

print_status() {
  local req_file="$1" qry_file="$2"
  local last_req last_qry
  last_req=$(tail -n +2 "$req_file" 2>/dev/null | tail -n1 || true)
  last_qry=$(tail -n +2 "$qry_file" 2>/dev/null | tail -n1 || true)
  local sreq rreq itreq ttqry
  if [[ -n "$last_req" ]]; then
    IFS=, read -r _ sreq rreq _ _ itreq _ _ _ _ _ _ <<<"$last_req"
  fi
  if [[ -n "$last_qry" ]]; then
    IFS=, read -r _ _ _ _ ttqry _ _ _ _ _ _ _ <<<"$last_qry"
  fi
  printf "[status] REQ sent=%s recv=%s itps=%s | QRY tps=%s\n" \
    "${sreq:--}" "${rreq:--}" "${itreq:--}" "${ttqry:--}"
}

echo "[watch] printing status every ${SNAPSHOT}s..."
while kill -0 ${REQ_PID} 2>/dev/null; do
  print_status "${REQ_CSV}" "${QRY_CSV}"
  sleep "${SNAPSHOT}"
done

wait ${REQ_PID} || true

echo "\n=== Summary (${RUN_ID}) ==="
final_req=$(tail -n +2 "${REQ_CSV}" 2>/dev/null | tail -n1 || true)
final_qry=$(tail -n +2 "${QRY_CSV}" 2>/dev/null | tail -n1 || true)
if [[ -n "$final_req" ]]; then
  IFS=, read -r _ sreq rreq errs ttreq itreq _ _ _ _ _ _ <<<"$final_req"
  echo "Requester: sent=${sreq} recv=${rreq} total_tps=${ttreq} errors=${errs}"
fi
if [[ -n "$final_qry" ]]; then
  IFS=, read -r _ ssent _ errs tt _ _ _ _ _ _ _ <<<"$final_qry"
  echo "Queryable: replies=${ssent} total_tps=${tt} errors=${errs}"
fi

echo "MQTT queries run complete. Artifacts at ${ART_DIR}"
