#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Multi-topic per-key subscribers + multi-topic publisher
# Default: both subs and pubs connect to router1 (7447)

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
def SNAPSHOT  "${SNAPSHOT:-1}"
def SHARE_TRANSPORT "${SHARE_TRANSPORT:-false}"
def RAMP_UP_SECS "${RAMP_UP_SECS:-5}"
ENGINE="${ENGINE:-zenoh}"

def TOPIC_PREFIX "${TOPIC_PREFIX:-bench/mtopic}"
def ENDPOINT_SUB "${ENDPOINT_SUB:-tcp/127.0.0.1:7447}"
def ENDPOINT_PUB "${ENDPOINT_PUB:-tcp/127.0.0.1:7447}"
# Optional: set ZENOH_MODE=client|peer to configure session mode via --connect
def ZENOH_MODE "${ZENOH_MODE:-}"
# Delay (seconds) between starting subscribers and publishers to let subs stabilize
def SUB_STARTUP_DELAY "${SUB_STARTUP_DELAY:-5}"

ART_DIR="artifacts/${RUN_ID}/fanout_multi_topic_perkey"
BIN="./target/release/mq-bench"

echo "[run_multi_topic_perkey] ${RUN_ID} (ENGINE=${ENGINE})"
echo "Dims: T=${TENANTS} R=${REGIONS} S=${SERVICES} K=${SHARDS} | subs=${SUBSCRIBERS} pubs=${PUBLISHERS} mapping=${MAPPING}"
echo "Load: payload=${PAYLOAD} rate_per_pub=${RATE} dur=${DURATION}s snapshot=${SNAPSHOT}s share_transport=${SHARE_TRANSPORT}"
echo "Paths: sub=${ENDPOINT_SUB} pub=${ENDPOINT_PUB} prefix=${TOPIC_PREFIX} mode=${ZENOH_MODE:-client}"
mkdir -p "${ART_DIR}"

build_release_if_needed "${BIN}"

SUB_CSV="${ART_DIR}/sub_agg.csv"
PUB_CSV="${ART_DIR}/mt_pub.csv"

echo "Starting per-key subscribers"
# Build connect args via helper
CONNECT_SUB_ARGS=()
make_connect_args sub CONNECT_SUB_ARGS
SHARE_FLAG=()
if [[ "${SHARE_TRANSPORT}" == "true" ]]; then SHARE_FLAG=(--share-transport); fi
RAMP_UP_FLAG=()
if [[ -n "${RAMP_UP_SECS}" ]] && [[ "${RAMP_UP_SECS}" != "0" ]]; then RAMP_UP_FLAG=(--ramp-up-secs "${RAMP_UP_SECS}"); fi
CMD_MTSUB=(
  "${BIN}" --snapshot-interval "${SNAPSHOT}" mt-sub
  "${CONNECT_SUB_ARGS[@]}"
  --topic-prefix "${TOPIC_PREFIX}"
  --tenants "${TENANTS}"
  --regions "${REGIONS}"
  --services "${SERVICES}"
  --shards "${SHARDS}"
  --subscribers "${SUBSCRIBERS}"
  --mapping "${MAPPING}"
  --duration "${DURATION}"
  "${RAMP_UP_FLAG[@]}"
  "${SHARE_FLAG[@]}"
  --csv "${SUB_CSV}"
)
print_cmd "${CMD_MTSUB[@]}" && echo "       1>$(printf %q "${ART_DIR}/mt_sub.log") 2>&1 &"
"${CMD_MTSUB[@]}" >"${ART_DIR}/mt_sub.log" 2>&1 &
SUB_PID=$!
trap 'echo "Stopping subscribers (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT

echo "Waiting ${SUB_STARTUP_DELAY}s for subscribers to stabilize..."
sleep "${SUB_STARTUP_DELAY}"

echo "Starting multi-topic publishers"
RATE_FLAG=()
if [[ -n "${RATE}" ]]; then RATE_FLAG=(--rate "${RATE}"); fi
if [[ "${SHARE_TRANSPORT}" == "true" ]]; then SHARE_FLAG=(--share-transport); else SHARE_FLAG=(); fi
RAMP_UP_FLAG=()
if [[ -n "${RAMP_UP_SECS}" ]] && [[ "${RAMP_UP_SECS}" != "0" ]]; then RAMP_UP_FLAG=(--ramp-up-secs "${RAMP_UP_SECS}"); fi
# Build connect args for publisher via helper
CONNECT_PUB_ARGS=()
make_connect_args pub CONNECT_PUB_ARGS
CMD_MTPUB=(
  "${BIN}" --snapshot-interval "${SNAPSHOT}" mt-pub
  "${CONNECT_PUB_ARGS[@]}"
  --topic-prefix "${TOPIC_PREFIX}"
  --tenants "${TENANTS}"
  --regions "${REGIONS}"
  --services "${SERVICES}"
  --shards "${SHARDS}"
  --publishers "${PUBLISHERS}"
  --mapping "${MAPPING}"
  --payload "${PAYLOAD}"
  "${RATE_FLAG[@]}"
  "${RAMP_UP_FLAG[@]}"
  --duration "${DURATION}"
  "${SHARE_FLAG[@]}"
  --csv "${PUB_CSV}"
)
print_cmd "${CMD_MTPUB[@]}" && echo "       1>$(printf %q "${ART_DIR}/mt_pub.log") 2>&1 &"
"${CMD_MTPUB[@]}" >"${ART_DIR}/mt_pub.log" 2>&1 &
PUB_PID=$!

echo "[watch] printing status every ${SNAPSHOT}s..."
watch_until_pub_exits ${PUB_PID} "${SUB_CSV}" "${PUB_CSV}"

wait ${PUB_PID} || true

echo "\n=== Summary (${RUN_ID}) ==="
summarize_common "${SUB_CSV}" "${PUB_CSV}"

echo "Multi-topic per-key scenario complete. Artifacts at ${ART_DIR}"
