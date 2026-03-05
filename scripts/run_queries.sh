#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Requester/Queryable scenario
# Default: both on router1 (7447). For cross-router, pass endpoints explicitly.
# Usage: scripts/run_queries.sh [RUN_ID] [QPS=200] [CONC=20] [TIMEOUT_MS=5000] [DURATION=20] [QRY_EP=tcp/127.0.0.1:7447] [REQ_EP=tcp/127.0.0.1:7447]

RUN_ID=${1:-run_$(date +%Y%m%d_%H%M%S)}
QPS=${2:-${QPS:-10000}}
CONC=${3:-${CONC:-32}}
DURATION=${4:-20}
TIMEOUT_MS=${5:-500}
REPLY_SIZE=${6:-1024}
SNAPSHOT=${7:-5}
def KEY_PREFIX "${KEY_PREFIX:-bench/qry}"
def PROC_DELAY "${PROC_DELAY:-0}"
def QRY_EP "${QRY_EP:-tcp/127.0.0.1:7447}"
def REQ_EP "${REQ_EP:-tcp/127.0.0.1:7447}"
ZENOH_MODE="${ZENOH_MODE:-}"

ART_DIR="artifacts/${RUN_ID}/queries"
BIN="./target/release/mq-bench"
PREFIX="${KEY_PREFIX}"
KEY_EXPR="${PREFIX}/item"

echo "[run_queries] Run ID: ${RUN_ID} | QPS=${QPS} CONC=${CONC} TIMEOUT=${TIMEOUT_MS}ms DURATION=${DURATION}s"
mkdir -p "${ART_DIR}"

if [[ ! -x "${BIN}" ]]; then
	echo "Building release binary..."
	cargo build --release
fi

QRY_CSV="${ART_DIR}/queryable.csv"
REQ_CSV="${ART_DIR}/requester.csv"

echo "Starting queryable on ${QRY_EP} serving prefix ${PREFIX}/**"
CONNECT_QRY_ARGS=(--endpoint "${QRY_EP}")
if [[ -n "${ZENOH_MODE}" ]]; then
	CONNECT_QRY_ARGS=(--connect "endpoint=${QRY_EP}" --connect "mode=${ZENOH_MODE}")
fi
CMD_QRY=(
	"${BIN}" --snapshot-interval "${SNAPSHOT}" qry
	"${CONNECT_QRY_ARGS[@]}"
	--serve-prefix "${PREFIX}/**"
	--reply-size "${REPLY_SIZE}"
	--proc-delay "${PROC_DELAY}"
	--csv "${QRY_CSV}"
)
print_cmd "${CMD_QRY[@]}" && echo "       1>$(printf %q "${ART_DIR}/qry.log") 2>&1 &"
"${CMD_QRY[@]}" >"${ART_DIR}/qry.log" 2>&1 &
QRY_PID=$!
trap 'echo "Stopping queryable (${QRY_PID})"; kill ${QRY_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running requester on ${REQ_EP} querying ${KEY_EXPR}"
QPS_FLAG=()
if [[ -n "${QPS}" ]] && (( QPS > 0 )); then QPS_FLAG=(--qps "${QPS}"); fi
CONNECT_REQ_ARGS=(--endpoint "${REQ_EP}")
if [[ -n "${ZENOH_MODE}" ]]; then
	CONNECT_REQ_ARGS=(--connect "endpoint=${REQ_EP}" --connect "mode=${ZENOH_MODE}")
fi
CMD_REQ=(
	"${BIN}" --snapshot-interval "${SNAPSHOT}" req
	"${CONNECT_REQ_ARGS[@]}"
	--key-expr "${KEY_EXPR}"
	"${QPS_FLAG[@]}"
	--concurrency "${CONC}"
	--timeout "${TIMEOUT_MS}"
	--duration "${DURATION}"
	--csv "${REQ_CSV}"
)
print_cmd "${CMD_REQ[@]}" && echo "       1>$(printf %q "${ART_DIR}/req.log") 2>&1 &"
"${CMD_REQ[@]}" >"${ART_DIR}/req.log" 2>&1 &
REQ_PID=$!

print_status() {
	local req_file="$1" qry_file="$2"
	local last_req last_qry
	last_req=$(tail -n +2 "$req_file" 2>/dev/null | tail -n1 || true)
	last_qry=$(tail -n +2 "$qry_file" 2>/dev/null | tail -n1 || true)
	local sreq rreq itreq ttqry
	if [[ -n "$last_req" ]]; then
		IFS=, read -r _ sreq rreq _ _ itreq _ _ _ _ _ _ _ _ <<<"$last_req"
	fi
	if [[ -n "$last_qry" ]]; then
		IFS=, read -r _ _ _ _ ttqry _ _ _ _ _ _ _ _ _ <<<"$last_qry"
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
	IFS=, read -r _ sreq rreq errs ttreq itreq _ _ _ _ _ _ _ _ <<<"$final_req"
	echo "Requester: sent=${sreq} recv=${rreq} total_tps=${ttreq} errors=${errs}"
fi
if [[ -n "$final_qry" ]]; then
	IFS=, read -r _ ssent _ errs tt _ _ _ _ _ _ _ _ _ <<<"$final_qry"
	echo "Queryable: replies=${ssent} total_tps=${tt} errors=${errs}"
fi

echo "Queries run complete. Artifacts at ${ART_DIR}"