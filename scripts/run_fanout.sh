#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# Fanout scenario: 1 publisher → N subscribers on a single router (router1:7447)
# Usage: scripts/run_fanout.sh [RUN_ID] [SUBS=4] [RATE=10000] [DURATION=30]

RUN_ID=${1:-${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}}
SUBS=${2:-${SUBS:-4}}
def RATE     "${RATE:-10000}"
def DURATION "${DURATION:-30}"
def PAYLOAD  "${PAYLOAD:-1024}"
def SNAPSHOT "${SNAPSHOT:-5}"

ART_DIR="artifacts/${RUN_ID}/fanout_singlesite"
BIN="./target/release/mq-bench"
ENDPOINT="${ENDPOINT:-tcp/127.0.0.1:7447}"
KEY="${KEY:-bench/topic}"

echo "[run_fanout] Run ID: ${RUN_ID} | SUBS=${SUBS} RATE=${RATE} DURATION=${DURATION}s"
mkdir -p "${ART_DIR}"

if [[ ! -x "${BIN}" ]]; then
	echo "Building release binary..."
	cargo build --release
fi

SUB_CSV="${ART_DIR}/sub_agg.csv"
PUB_CSV="${ART_DIR}/pub.csv"

echo "Starting ${SUBS} subscribers on ${ENDPOINT} → ${KEY} (aggregated CSV)"
"${BIN}" --snapshot-interval "${SNAPSHOT}" sub \
	--endpoint "${ENDPOINT}" \
	--expr "${KEY}" \
	--subscribers "${SUBS}" \
	--csv "${SUB_CSV}" \
	>"${ART_DIR}/sub.log" 2>&1 &
SUB_PID=$!
trap 'echo "Stopping subscribers (${SUB_PID})"; kill ${SUB_PID} >/dev/null 2>&1 || true' EXIT

sleep 1

echo "Running publisher on ${ENDPOINT} → ${KEY}"
RATE_FLAG=()
if [[ -n "${RATE}" ]] && (( RATE > 0 )); then RATE_FLAG=(--rate "${RATE}"); fi
"${BIN}" --snapshot-interval "${SNAPSHOT}" pub \
	--endpoint "${ENDPOINT}" \
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

echo "Fanout run complete. Artifacts at ${ART_DIR}"