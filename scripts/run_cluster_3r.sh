#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Single router scenario: sub and pub on router1 (7447)
# Usage: scripts/run_cluster_3r.sh [RUN_ID]

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
ART_DIR="artifacts/${RUN_ID}/cluster_3r"
BIN="./target/release/mq-bench"
SUB_ENDPOINT="${SUB_ENDPOINT:-tcp/127.0.0.1:7447}"
PUB_ENDPOINT="${PUB_ENDPOINT:-tcp/127.0.0.1:7447}"
KEY="${KEY:-bench/topic}"
def PAYLOAD "${PAYLOAD:-1024}"
def RATE     "${RATE:-10000}"
def DURATION "${DURATION:-20}"
def SNAPSHOT "${SNAPSHOT:-5}"
ZENOH_MODE="${ZENOH_MODE:-}"

echo "[run_cluster_3r] Run ID: ${RUN_ID}"
mkdir -p "${ART_DIR}"

if [[ ! -x "${BIN}" ]]; then
  echo "Building release binary..."
  cargo build --release
fi

SUB_CSV="${ART_DIR}/sub.csv"
PUB_CSV="${ART_DIR}/pub.csv"

echo "Starting subscriber on ${SUB_ENDPOINT} → ${KEY}"
CONNECT_SUB_ARGS=(--endpoint "${SUB_ENDPOINT}")
if [[ -n "${ZENOH_MODE}" ]]; then
  CONNECT_SUB_ARGS=(--connect "endpoint=${SUB_ENDPOINT}" --connect "mode=${ZENOH_MODE}")
fi
CMD_SUB=(
  "${BIN}" --snapshot-interval "${SNAPSHOT}" sub
  "${CONNECT_SUB_ARGS[@]}"
  --expr "${KEY}"
  --csv "${SUB_CSV}"
)
print_cmd "${CMD_SUB[@]}" && echo "       1>$(printf %q "${ART_DIR}/sub.log") 2>&1 &"
"${CMD_SUB[@]}" >"${ART_DIR}/sub.log" 2>&1 &
SUB_PID=$!
trap 'echo "Stopping subscriber (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running publisher on ${PUB_ENDPOINT} → ${KEY} (payload=${PAYLOAD}, rate=${RATE}, duration=${DURATION}s, snap=${SNAPSHOT}s)"
CONNECT_PUB_ARGS=(--endpoint "${PUB_ENDPOINT}")
if [[ -n "${ZENOH_MODE}" ]]; then
  CONNECT_PUB_ARGS=(--connect "endpoint=${PUB_ENDPOINT}" --connect "mode=${ZENOH_MODE}")
fi
CMD_PUB=(
  "${BIN}" --snapshot-interval "${SNAPSHOT}" pub
  "${CONNECT_PUB_ARGS[@]}"
  --topic-prefix "${KEY}"
  --payload "${PAYLOAD}"
  ${RATE:+--rate "${RATE}"}
  --duration "${DURATION}"
  --csv "${PUB_CSV}"
)
print_cmd "${CMD_PUB[@]}" && echo "       1>$(printf %q "${ART_DIR}/pub.log") 2>&1 &"
"${CMD_PUB[@]}" >"${ART_DIR}/pub.log" 2>&1 &
PUB_PID=$!

# Status watcher
print_status() {
  local sub_file="$1" pub_file="$2"
  local last_sub last_pub
  last_sub=$(tail -n +2 "$sub_file" 2>/dev/null | tail -n1 || true)
  last_pub=$(tail -n +2 "$pub_file" 2>/dev/null | tail -n1 || true)
  local spub itpub ttpub rsub itsub p99sub conns_pub active_pub conns_sub active_sub
  if [[ -n "$last_pub" ]]; then
    IFS=, read -r _ spub _ epub ttpub itpub _ _ _ _ _ _ conns_pub active_pub <<<"$last_pub"
  fi
  if [[ -n "$last_sub" ]]; then
    IFS=, read -r _ _ rsub _ _ itsub _ _ p99sub _ _ _ conns_sub active_sub <<<"$last_sub"
  fi
  printf "[status] PUB sent=%s itps=%s tps=%s conn=%s/%s | SUB recv=%s itps=%s p99=%.2fms conn=%s/%s\n" \
    "${spub:--}" "${itpub:--}" "${ttpub:--}" "${active_pub:--}" "${conns_pub:--}" \
    "${rsub:--}" "${itsub:--}" "$(awk -v n="${p99sub:-0}" 'BEGIN{printf (n/1e6)}')" \
    "${active_sub:--}" "${conns_sub:--}"
}

echo "[watch] printing status every ${SNAPSHOT}s..."
while kill -0 ${PUB_PID} 2>/dev/null; do
  print_status "${SUB_CSV}" "${PUB_CSV}"
  sleep "${SNAPSHOT}"
done

wait ${PUB_PID} || true

# Final summary
echo "\n=== Summary (${RUN_ID}) ==="
final_pub=$(tail -n +2 "${PUB_CSV}" 2>/dev/null | tail -n1 || true)
final_sub=$(tail -n +2 "${SUB_CSV}" 2>/dev/null | tail -n1 || true)
if [[ -n "$final_pub" ]]; then
  IFS=, read -r _ tsent _ terr tt _ _ _ _ _ _ _ conns_pub active_pub <<<"$final_pub"
  echo "Publisher: sent=${tsent} total_tps=${tt} connections=${conns_pub:-0} active=${active_pub:-0}"
fi
if [[ -n "$final_sub" ]]; then
  IFS=, read -r _ _ rcv _ tps _ p50 p95 p99 _ _ _ conns_sub active_sub <<<"$final_sub"
  printf "Subscriber: recv=%s total_tps=%.2f p50=%.2fms p95=%.2fms p99=%.2fms connections=%s active=%s\n" \
    "$rcv" "$tps" "$(awk -v n="$p50" 'BEGIN{printf (n/1e6)}')" \
    "$(awk -v n="$p95" 'BEGIN{printf (n/1e6)}')" "$(awk -v n="$p99" 'BEGIN{printf (n/1e6)}')" \
    "${conns_sub:-0}" "${active_sub:-0}"
fi

echo "Run complete. Artifacts at ${ART_DIR}"