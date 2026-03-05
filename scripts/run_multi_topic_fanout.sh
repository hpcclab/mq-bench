#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Multi-topic fanout: single-process multi-publisher over many keys → single wildcard subscriber
# Default: both subs and pubs connect to router1 (7447)
# Usage: scripts/run_multi_topic_fanout.sh [RUN_ID]

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}

# Dimensions and mapping (defaults align with TASKS.md examples)
def TENANTS   "${TENANTS:-10}"
def REGIONS   "${REGIONS:-2}"
def SERVICES  "${SERVICES:-5}"
def SHARDS    "${SHARDS:-10}"
def PUBLISHERS "${PUBLISHERS:-100}"
def MAPPING   "${MAPPING:-hash}"   # values: hash | mdim

# Workload
def PAYLOAD   "${PAYLOAD:-1024}"
def RATE      "${RATE:-10}"        # per logical publisher (msg/s). <=0 for max speed
def DURATION  "${DURATION:-30}"
def SNAPSHOT  "${SNAPSHOT:-5}"
def SHARE_TRANSPORT "${SHARE_TRANSPORT:-false}"
ENGINE="${ENGINE:-zenoh}"

# Topology and keys
def TOPIC_PREFIX "${TOPIC_PREFIX:-bench/mtopic}"
def ENDPOINT_SUB "${ENDPOINT_SUB:-tcp/127.0.0.1:7447}"
def ENDPOINT_PUB "${ENDPOINT_PUB:-tcp/127.0.0.1:7447}"
def ZENOH_MODE "${ZENOH_MODE:-}"

ART_DIR="artifacts/${RUN_ID}/fanout_multi_topic"
BIN="./target/release/mq-bench"

hdr "[run_multi_topic_fanout] ${RUN_ID} (ENGINE=${ENGINE})"
echo "Dims: T=${TENANTS} R=${REGIONS} S=${SERVICES} K=${SHARDS} | pubs=${PUBLISHERS} mapping=${MAPPING}"
echo "Load: payload=${PAYLOAD} rate_per_pub=${RATE} dur=${DURATION}s snapshot=${SNAPSHOT}s share_transport=${SHARE_TRANSPORT}"
echo "Paths: sub=${ENDPOINT_SUB} pub=${ENDPOINT_PUB} prefix=${TOPIC_PREFIX} mode=${ZENOH_MODE:-client}"
mkdir -p "${ART_DIR}"

build_release_if_needed "${BIN}"

SUB_CSV="${ART_DIR}/sub_agg.csv"
PUB_CSV="${ART_DIR}/mt_pub.csv"

echo "Starting wildcard subscriber → ${TOPIC_PREFIX}/** (aggregated CSV)"
SUB_EXPR="${TOPIC_PREFIX}/**"
# Use shared starter (aggregates when subscribers>1; here use 1 to get CSV schema)
start_sub SUB_PID "${SUB_EXPR}" 1 "${SUB_CSV}" "${ART_DIR}/sub.log"
trap 'echo "Stopping subscriber (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running multi-topic publisher → ${TOPIC_PREFIX}/t*/r*/svc*/k*"
RATE_FLAG=()
if [[ -n "${RATE}" ]]; then RATE_FLAG=(--rate "${RATE}"); fi
SHARE_FLAG=()
if [[ "${SHARE_TRANSPORT}" == "true" ]]; then SHARE_FLAG=(--share-transport); fi
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
  --duration "${DURATION}"
  "${SHARE_FLAG[@]}"
  --csv "${PUB_CSV}"
)
print_cmd "${CMD_MTPUB[@]}" && echo "       1>$(printf %q "${ART_DIR}/mt_pub.log") 2>&1 &"
"${CMD_MTPUB[@]}" >"${ART_DIR}/mt_pub.log" 2>&1 &
PUB_PID=$!

watch_until_pub_exits ${PUB_PID} "${SUB_CSV}" "${PUB_CSV}"

wait ${PUB_PID} || true

echo "\n=== Summary (${RUN_ID}) ==="
summarize_common "${SUB_CSV}" "${PUB_CSV}"

echo "Multi-topic fanout run complete. Artifacts at ${ART_DIR}"
