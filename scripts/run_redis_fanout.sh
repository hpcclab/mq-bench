#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Redis Fanout scenario: 1 publisher → N subscribers on a single Redis instance
# Usage: scripts/run_redis_fanout.sh [RUN_ID] [SUBS=4] [RATE=10000] [DURATION=30] [PAYLOAD=1024]

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
SUBS=${2:-${SUBS:-4}}
def RATE     "${RATE:-10000}"
def DURATION "${DURATION:-30}"
def PAYLOAD  "${PAYLOAD:-1024}"
def SNAPSHOT "${SNAPSHOT:-5}"
def REDIS_URL "${REDIS_URL:-redis://127.0.0.1:6379}"
def KEY "${KEY:-bench/topic}"

ART_DIR="artifacts/${RUN_ID}/fanout_redis"
BIN="./target/release/mq-bench"

echo "[run_redis_fanout] Run ID: ${RUN_ID} | SUBS=${SUBS} RATE=${RATE} DURATION=${DURATION}s KEY=${KEY}"
mkdir -p "${ART_DIR}"

echo "Building release binary (redis feature)..."
cargo build --release --features transport-redis

SUB_CSV="${ART_DIR}/sub_agg.csv"
PUB_CSV="${ART_DIR}/pub.csv"

# Optional: choose lock-free single-thread publisher
CONNECT_OPTS=(--connect "url=${REDIS_URL}")
if [[ "${PUB_MODE:-}" == "single" ]]; then
  CONNECT_OPTS+=(--connect "pub_mode=single")
fi

echo "Starting ${SUBS} subscribers on redis → ${KEY} (aggregated CSV)"
"${BIN}" --snapshot-interval "${SNAPSHOT}" sub \
  --engine redis \
  "${CONNECT_OPTS[@]}" \
  --expr "${KEY}" \
  --subscribers "${SUBS}" \
  --csv "${SUB_CSV}" \
  >"${ART_DIR}/sub.log" 2>&1 &
SUB_PID=$!
trap 'echo "Stopping subscribers (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running publisher on redis → ${KEY}"
RATE_FLAG=()
if [[ -n "${RATE}" ]] && (( RATE > 0 )); then RATE_FLAG=(--rate "${RATE}"); fi
"${BIN}" --snapshot-interval "${SNAPSHOT}" pub \
  --engine redis \
  "${CONNECT_OPTS[@]}" \
  --topic-prefix "${KEY}" \
  --payload "${PAYLOAD}" \
  "${RATE_FLAG[@]}" \
  --duration "${DURATION}" \
  --csv "${PUB_CSV}" \
  >"${ART_DIR}/pub.log" 2>&1 &
PUB_PID=$!

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

echo "[watch] printing status every ${SNAPSHOT}s..."
while kill -0 ${PUB_PID} 2>/dev/null; do
  print_status "${SUB_CSV}" "${PUB_CSV}"
  sleep "${SNAPSHOT}"
done

wait ${PUB_PID} || true

echo "\n=== Summary (${RUN_ID}) ==="
final_pub=$(tail -n +2 "${PUB_CSV}" 2>/dev/null | tail -n1 || true)
final_sub=$(tail -n +2 "${SUB_CSV}" 2>/dev/null | tail -n1 || true)
if [[ -n "$final_pub" ]]; then
  IFS=, read -r _ tsent _ terr tt _ _ _ _ _ _ _ <<<"$final_pub"
  echo "Publisher: sent=${tsent} total_tps=${tt}"
fi
if [[ -n "$final_sub" ]]; then
  IFS=, read -r _ _ rcv _ tps _ p50 p95 p99 _ _ _ <<<"$final_sub"
  printf "Subscriber: recv=%s total_tps=%.2f p50=%.2fms p95=%.2fms p99=%.2fms\n" \
    "$rcv" "$tps" "$(awk -v n="$p50" 'BEGIN{printf (n/1e6)}')" \
    "$(awk -v n="$p95" 'BEGIN{printf (n/1e6)}')" "$(awk -v n="$p99" 'BEGIN{printf (n/1e6)}')"
fi

echo "Redis fanout run complete. Artifacts at ${ART_DIR}"
