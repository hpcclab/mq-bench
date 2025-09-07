#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Redis Requester/Queryable scenario
# Usage: scripts/run_redis_queries.sh [RUN_ID] [QPS=200] [CONC=20] [TIMEOUT_MS=5000] [DURATION=20] [REPLY_SIZE=1024]

RUN_ID=${1:-run_$(date +%Y%m%d_%H%M%S)}
QPS=${2:-${QPS:-200}}
CONC=${3:-${CONC:-20}}
TIMEOUT_MS=${4:-${TIMEOUT_MS:-2000}}
DURATION=${5:-${DURATION:-20}}
REPLY_SIZE=${6:-${REPLY_SIZE:-512}}
def SNAPSHOT "${SNAPSHOT:-5}"
def REDIS_URL "${REDIS_URL:-redis://127.0.0.1:6379}"
def KEY_EXPR "${KEY_EXPR:-bench/q}"   # Exact subject for req/qry

ART_DIR="artifacts/${RUN_ID}/redis_queries"
BIN="./target/release/mq-bench"

echo "[run_redis_queries] Run ID: ${RUN_ID} | QPS=${QPS} CONC=${CONC} TIMEOUT=${TIMEOUT_MS}ms DURATION=${DURATION}s KEY=${KEY_EXPR}"
mkdir -p "${ART_DIR}"

echo "Building release binary (redis feature)..."
cargo build --release --features transport-redis

QRY_CSV="${ART_DIR}/queryable.csv"
REQ_CSV="${ART_DIR}/requester.csv"

echo "Starting queryable on redis serving key '${KEY_EXPR}'"
"${BIN}" --snapshot-interval "${SNAPSHOT}" qry \
  --engine redis \
  --connect "url=${REDIS_URL}" \
  --serve-prefix "${KEY_EXPR}" \
  --reply-size "${REPLY_SIZE}" \
  --proc-delay "0" \
  --csv "${QRY_CSV}" \
  >"${ART_DIR}/qry.log" 2>&1 &
QRY_PID=$!
trap 'echo "Stopping queryable (${QRY_PID})"; kill ${QRY_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running requester on redis for key '${KEY_EXPR}'"
QPS_FLAG=()
if [[ -n "${QPS}" ]] && (( QPS > 0 )); then QPS_FLAG=(--qps "${QPS}"); fi
"${BIN}" --snapshot-interval "${SNAPSHOT}" req \
  --engine redis \
  --connect "url=${REDIS_URL}" \
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

echo "Redis queries run complete. Artifacts at ${ART_DIR}"
