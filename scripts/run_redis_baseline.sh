#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Local Redis pub/sub baseline. Redis must be up (scripts/compose_up.sh).
# Usage: scripts/run_redis_baseline.sh [RUN_ID]

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
def PAYLOAD "${PAYLOAD:-1024}"
def RATE     "${RATE:-10000}"
def DURATION "${DURATION:-20}"
def SNAPSHOT "${SNAPSHOT:-5}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
KEY="${KEY:-bench/topic}"

ART_DIR="artifacts/${RUN_ID}/redis_baseline"
BIN="./target/release/mq-bench"

echo "[run_redis_baseline] Run ID: ${RUN_ID}"
mkdir -p "${ART_DIR}"

echo "Building release binary (redis feature)..."
cargo build --release --features transport-redis

SUB_CSV="${ART_DIR}/sub.csv"
PUB_CSV="${ART_DIR}/pub.csv"

echo "Starting subscriber on redis → ${KEY}"
"${BIN}" --snapshot-interval "${SNAPSHOT}" sub --engine redis --connect "url=${REDIS_URL}" --expr "${KEY}" --csv "${SUB_CSV}" >"${ART_DIR}/sub.log" 2>&1 &
SUB_PID=$!
trap 'echo "Stopping subscriber (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running publisher on redis → ${KEY} (payload=${PAYLOAD}, rate=${RATE}, duration=${DURATION}s, snap=${SNAPSHOT}s)"
RATE_FLAG=()
if [[ -n "${RATE}" ]] && (( RATE > 0 )); then RATE_FLAG=(--rate "${RATE}"); fi
"${BIN}" --snapshot-interval "${SNAPSHOT}" pub --engine redis --connect "url=${REDIS_URL}" --topic-prefix "${KEY}" --payload "${PAYLOAD}" "${RATE_FLAG[@]}" --duration "${DURATION}" --csv "${PUB_CSV}" >"${ART_DIR}/pub.log" 2>&1 &
PUB_PID=$!

# Status watcher (prints every SNAPSHOT seconds until publisher exits)
print_status() {
  local sub_file="$1" pub_file="$2"
  local last_sub last_pub
  last_sub=$(tail -n +2 "$sub_file" 2>/dev/null | tail -n1 || true)
  last_pub=$(tail -n +2 "$pub_file" 2>/dev/null | tail -n1 || true)
  local sts spub rpub epub ttpub itpub p50sub p95sub p99sub itsub rsub
  if [[ -n "$last_pub" ]]; then
    IFS=, read -r _ts spub _rpub epub ttpub itpub _ _ _ _ _ _ <<<"$last_pub"
  fi
  if [[ -n "$last_sub" ]]; then
    IFS=, read -r _ts _ssub rsub _esub _ttsub itsub _p50 _p95 p99sub _min _max _mean <<<"$last_sub"
  fi
  printf "[status] PUB sent=%s itps=%s tps=%s | SUB recv=%s itps=%s p99=%.2fms\n" \
    "${spub:--}" "${itpub:--}" "${ttpub:--}" \
    "${rsub:--}" "${itsub:--}" "$(awk -v n="${p99sub:-0}" 'BEGIN{printf (n/1e6)}')"
}

echo "[watch] printing status every ${SNAPSHOT}s..."
while kill -0 ${PUB_PID} 2>/dev/null; do
  print_status "${SUB_CSV}" "${PUB_CSV}"
  sleep "${SNAPSHOT}"

done

wait ${PUB_PID} || true

# Final summary
echo "=== Summary (${RUN_ID}) ==="
final_pub=$(tail -n +2 "${PUB_CSV}" 2>/dev/null | tail -n1 || true)
final_sub=$(tail -n +2 "${SUB_CSV}" 2>/dev/null | tail -n1 || true)
if [[ -n "$final_pub" ]]; then
  IFS=, read -r _ tsent trecv terr tt it _ _ _ _ _ _ <<<"$final_pub"
  echo "Publisher: sent=${tsent} total_tps=${tt}"
fi
if [[ -n "$final_sub" ]]; then
  IFS=, read -r _ _ss rcv _e tps itps p50 p95 p99 _min _max mean <<<"$final_sub"
  printf "Subscriber: recv=%s total_tps=%.2f p50=%.2fms p95=%.2fms p99=%.2fms\n" \
    "$rcv" "$tps" "$(awk -v n="$p50" 'BEGIN{printf (n/1e6)}')" \
    "$(awk -v n="$p95" 'BEGIN{printf (n/1e6)}')" "$(awk -v n="$p99" 'BEGIN{printf (n/1e6)}')"
fi

echo "Run complete. Artifacts at ${ART_DIR}"
