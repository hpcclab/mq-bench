#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Multi-topic per-key subscribers + multi-topic publisher
# Default cross-router: subs on router2 (7448), pubs on router3 (7449)

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}

def TENANTS   "${TENANTS:-10}"
def REGIONS   "${REGIONS:-2}"
def SERVICES  "${SERVICES:-5}"
def SHARDS    "${SHARDS:-10}"
def SUBSCRIBERS "${SUBSCRIBERS:-100}"
def PUBLISHERS  "${PUBLISHERS:-100}"
def MAPPING   "${MAPPING:-mdim}"

def PAYLOAD   "${PAYLOAD:-1024}"
def RATE      "${RATE:-10}"
def DURATION  "${DURATION:-30}"
def SNAPSHOT  "${SNAPSHOT:-5}"
def SHARE_TRANSPORT "${SHARE_TRANSPORT:-true}"

def TOPIC_PREFIX "${TOPIC_PREFIX:-bench/mtopic}"
def ENDPOINT_SUB "${ENDPOINT_SUB:-tcp/127.0.0.1:7447}"
def ENDPOINT_PUB "${ENDPOINT_PUB:-tcp/127.0.0.1:7448}"
# Optional: set ZENOH_MODE=client|peer to configure session mode via --connect
def ZENOH_MODE "${ZENOH_MODE:-}"

ART_DIR="artifacts/${RUN_ID}/fanout_multi_topic_perkey"
BIN="./target/release/mq-bench"

echo "[run_multi_topic_perkey] ${RUN_ID}"
echo "Dims: T=${TENANTS} R=${REGIONS} S=${SERVICES} K=${SHARDS} | subs=${SUBSCRIBERS} pubs=${PUBLISHERS} mapping=${MAPPING}"
echo "Load: payload=${PAYLOAD} rate_per_pub=${RATE} dur=${DURATION}s snapshot=${SNAPSHOT}s share_transport=${SHARE_TRANSPORT}"
echo "Paths: sub=${ENDPOINT_SUB} pub=${ENDPOINT_PUB} prefix=${TOPIC_PREFIX} mode=${ZENOH_MODE:-client}"
mkdir -p "${ART_DIR}"

if [[ ! -x "${BIN}" ]]; then
  echo "Building release binary..."
  cargo build --release
fi

SUB_CSV="${ART_DIR}/sub_agg.csv"
PUB_CSV="${ART_DIR}/mt_pub.csv"

echo "Starting per-key subscribers on ${ENDPOINT_SUB}"
# Build connect args (prefer --connect to include mode when provided)
CONNECT_SUB_ARGS=(--endpoint "${ENDPOINT_SUB}")
if [[ -n "${ZENOH_MODE}" ]]; then
  CONNECT_SUB_ARGS=(--connect "endpoint=${ENDPOINT_SUB}" --connect "mode=${ZENOH_MODE}")
fi
SHARE_FLAG=()
if [[ "${SHARE_TRANSPORT}" == "true" ]]; then SHARE_FLAG=(--share-transport); fi
"${BIN}" --snapshot-interval "${SNAPSHOT}" mt-sub \
  "${CONNECT_SUB_ARGS[@]}" \
  --topic-prefix "${TOPIC_PREFIX}" \
  --tenants "${TENANTS}" \
  --regions "${REGIONS}" \
  --services "${SERVICES}" \
  --shards "${SHARDS}" \
  --subscribers "${SUBSCRIBERS}" \
  --mapping "${MAPPING}" \
  --duration "${DURATION}" \
  "${SHARE_FLAG[@]}" \
  --csv "${SUB_CSV}" \
  >"${ART_DIR}/mt_sub.log" 2>&1 &
SUB_PID=$!
trap 'echo "Stopping subscribers (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Starting multi-topic publishers on ${ENDPOINT_PUB}"
RATE_FLAG=()
if [[ -n "${RATE}" ]]; then RATE_FLAG=(--rate "${RATE}"); fi
if [[ "${SHARE_TRANSPORT}" == "true" ]]; then SHARE_FLAG=(--share-transport); else SHARE_FLAG=(); fi
# Build connect args for publisher
CONNECT_PUB_ARGS=(--endpoint "${ENDPOINT_PUB}")
if [[ -n "${ZENOH_MODE}" ]]; then
  CONNECT_PUB_ARGS=(--connect "endpoint=${ENDPOINT_PUB}" --connect "mode=${ZENOH_MODE}")
fi
"${BIN}" --snapshot-interval "${SNAPSHOT}" mt-pub \
  "${CONNECT_PUB_ARGS[@]}" \
  --topic-prefix "${TOPIC_PREFIX}" \
  --tenants "${TENANTS}" \
  --regions "${REGIONS}" \
  --services "${SERVICES}" \
  --shards "${SHARDS}" \
  --publishers "${PUBLISHERS}" \
  --mapping "${MAPPING}" \
  --payload "${PAYLOAD}" \
  "${RATE_FLAG[@]}" \
  --duration "${DURATION}" \
  "${SHARE_FLAG[@]}" \
  --csv "${PUB_CSV}" \
  >"${ART_DIR}/mt_pub.log" 2>&1 &
PUB_PID=$!

echo "[watch] printing status every ${SNAPSHOT}s..."
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

echo "Multi-topic per-key scenario complete. Artifacts at ${ART_DIR}"
