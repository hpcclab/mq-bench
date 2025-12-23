#!/bin/bash
set -euo pipefail

# Test script to generate summary.csv from existing failure test artifacts
# Usage: ./scripts/test_summary_from_artifacts.sh [output_csv]

REPO_ROOT="/home/cc/mq-bench"
SUMMARY_CSV="${1:-${REPO_ROOT}/results/test_failure_summary.csv}"
IGNORE_START_SECS=0
IGNORE_END_SECS=0

# Failure test params
SUB_MTTF=10
SUB_MTTR=2
SUB_CRASH_COUNT=10
PUB_CRASH_LOOP=0
PUB_MTTF=10
PUB_MTTR=2
PUB_CRASH_COUNT=10

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Create header
mkdir -p "$(dirname "${SUMMARY_CSV}")"
echo "transport,payload,rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,avg_latency_ms,pub_tps,sent,recv,errors,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes,sub_mttf,sub_mttr,sub_crash_count,pub_crash_loop,pub_mttf,pub_mttr,pub_crash_count" > "${SUMMARY_CSV}"

append_from_csv() {
  local transport="$1" payload="$2" rate="$3" run_id="$4" sub_csv="$5"
  local art_dir
  art_dir=$(dirname "$sub_csv")
  
  # Extract metrics using awk
  local metrics
  metrics=$(awk -F, -v ignore_start="${IGNORE_START_SECS}" -v ignore_end="${IGNORE_END_SECS}" '
    NR == 1 { next }
    {
        ts = $1 + 0
        if (min_ts == "" || ts < min_ts) min_ts = ts
        if (max_ts == "" || ts > max_ts) max_ts = ts
        row_ts[NR] = ts
        row_recv[NR] = $3 + 0
        row_sent[NR] = $2 + 0
        row_err[NR] = $4 + 0
        row_p50[NR] = $7 + 0
        row_p95[NR] = $8 + 0
        row_p99[NR] = $9 + 0
        row_mean[NR] = $12 + 0
        total_rows = NR
    }
    END {
        window_start = min_ts + ignore_start
        window_end = max_ts - ignore_end
        for (i = 2; i <= total_rows; i++) {
            ts = row_ts[i]
            if (ts >= window_start && ts <= window_end && row_recv[i] > 0) {
                if (first_ts == "") {
                    first_ts = ts; first_recv = row_recv[i]; first_sent = row_sent[i]; first_err = row_err[i]
                }
                last_ts = ts; last_recv = row_recv[i]; last_sent = row_sent[i]; last_err = row_err[i]
                sum_p50 += row_p50[i]; sum_p95 += row_p95[i]; sum_p99 += row_p99[i]; sum_mean += row_mean[i]
                count++
            }
        }
        if (count == 0) { print "0.00,0,0,0,0,0,0,0,0,0,0"; exit }
        duration = last_ts - first_ts
        tps = (duration > 0) ? (last_recv - first_recv) / duration : 0
        printf "%.2f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%d,%d,%d", tps, sum_p50/count, sum_p95/count, sum_p99/count, sum_mean/count, last_sent-first_sent, last_recv-first_recv, last_err-first_err, count, first_ts, last_ts
    }
  ' "${sub_csv}")
  
  local tps avg_p50_ns avg_p95_ns avg_p99_ns avg_mean_ns sent recv err steady_rows start_ts end_ts
  IFS=, read -r tps avg_p50_ns avg_p95_ns avg_p99_ns avg_mean_ns sent recv err steady_rows start_ts end_ts <<<"${metrics}"
  
  # Convert ns to ms
  local p50_ms p95_ms p99_ms avg_ms
  p50_ms=$(awk -v n="${avg_p50_ns}" 'BEGIN{if(n==""||n==0){print ""}else{printf("%.3f", n/1e6)}}')
  p95_ms=$(awk -v n="${avg_p95_ns}" 'BEGIN{if(n==""||n==0){print ""}else{printf("%.3f", n/1e6)}}')
  p99_ms=$(awk -v n="${avg_p99_ns}" 'BEGIN{if(n==""||n==0){print ""}else{printf("%.3f", n/1e6)}}')
  avg_ms=$(awk -v n="${avg_mean_ns}" 'BEGIN{if(n==""||n==0){print ""}else{printf("%.3f", n/1e6)}}')
  
  log "Processed: ${run_id} - TPS=${tps} p50=${p50_ms}ms p99=${p99_ms}ms rows=${steady_rows}"
  
  echo "${transport},${payload},${rate},${run_id},${tps},${p50_ms},${p95_ms},${p99_ms},${avg_ms},,${sent},${recv},${err},${art_dir},,,,,${SUB_MTTF},${SUB_MTTR},${SUB_CRASH_COUNT},${PUB_CRASH_LOOP},${PUB_MTTF},${PUB_MTTR},${PUB_CRASH_COUNT}" >> "${SUMMARY_CSV}"
}

# Process existing failure test artifacts
count=0
for dir in "${REPO_ROOT}"/artifacts/mt_mqtt_failure_*; do
  [[ -d "$dir" ]] || continue
  for csv in "${dir}"/mt_sub_qos*.csv; do
    [[ -f "$csv" ]] || continue
    # Extract info from path
    ts=$(basename "$dir" | sed 's/mt_mqtt_failure_//')
    qos=$(basename "$csv" | sed 's/mt_sub_qos\([0-9]*\)\.csv/\1/')
    run_id="failure_test_${ts}_q${qos}"
    
    log "Processing: $csv"
    append_from_csv "mqtt_mosquitto_q${qos}_failure" "1024" "100" "${run_id}" "${csv}"
    ((count++)) || true
  done
done

echo ""
echo "Processed ${count} failure test artifacts"
echo "Summary written to: ${SUMMARY_CSV}"
echo ""
echo "Preview:"
head -5 "${SUMMARY_CSV}"
