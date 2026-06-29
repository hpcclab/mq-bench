#!/usr/bin/env bash
set -euo pipefail

# Collect docker stats from a remote host via SSH and stream to a local CSV file.
# Usage:
#   ./collect_remote_docker_stats.sh <remote_host> <output_csv> <duration_seconds> [ssh_user]
#   ./collect_remote_docker_stats.sh <remote_host> <output_csv> <duration_seconds> --extended
#   ./collect_remote_docker_stats.sh <remote_host> <output_csv> <duration_seconds> --extended --containers "router1"
#
# Notes:
# - Existing callers can keep using the legacy 5-column format unchanged.
# - `--extended` adds numeric network RX/TX byte counters for bandwidth analysis.
# - `--containers` limits collection to specific docker container names on the remote host.

usage() {
  echo "Usage: $0 <remote_host> <output_csv> <duration_seconds> [ssh_user] [--extended] [--containers \"name1 name2\"]" >&2
}

if [[ $# -lt 3 ]]; then
  usage
  exit 2
fi

REMOTE_HOST="$1"
OUTPUT_CSV="$2"
DURATION="${3:-60}"
shift 3

SSH_USER="ubuntu"
EXTENDED=0
CONTAINERS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --extended|--include-network)
      EXTENDED=1
      ;;
    --containers)
      shift
      if [[ -n "${1:-}" ]]; then
        IFS=' ' read -r -a CONTAINERS <<<"${1}"
      fi
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      SSH_USER="$1"
      ;;
  esac
  shift || true
done

SSH_DEST="${REMOTE_HOST}"
if [[ "${REMOTE_HOST}" != *"@"* ]]; then
  SSH_DEST="${SSH_USER}@${REMOTE_HOST}"
fi

mkdir -p "$(dirname "${OUTPUT_CSV}")"

if [[ ${EXTENDED} -eq 1 ]]; then
  echo "timestamp,container,cpu_perc,mem_perc,mem_usage,net_rx_b,net_tx_b" > "${OUTPUT_CSV}"
else
  echo "timestamp,container,cpu_perc,mem_perc,mem_usage" > "${OUTPUT_CSV}"
fi

ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${SSH_DEST}" bash -s -- "${DURATION}" "${EXTENDED}" "${CONTAINERS[@]}" >> "${OUTPUT_CSV}" <<'EOF'
set -euo pipefail

DURATION="$1"
EXTENDED="$2"
shift 2
CONTAINERS=("$@")

to_bytes() {
  local raw="${1:-}"
  awk -v v="${raw}" '
    BEGIN {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
      if (v == "" || v == "-") {
        print 0
        exit
      }
      num = v + 0
      unit = v
      gsub(/[0-9.]/, "", unit)

      if (unit == "" || unit == "B") mult = 1
      else if (unit == "kB" || unit == "KB") mult = 1000
      else if (unit == "MB") mult = 1000 * 1000
      else if (unit == "GB") mult = 1000 * 1000 * 1000
      else if (unit == "TB") mult = 1000 * 1000 * 1000 * 1000
      else if (unit == "KiB") mult = 1024
      else if (unit == "MiB") mult = 1024 * 1024
      else if (unit == "GiB") mult = 1024 * 1024 * 1024
      else if (unit == "TiB") mult = 1024 * 1024 * 1024 * 1024
      else mult = 1

      printf "%.0f\n", num * mult
    }
  '
}

end=$(( $(date +%s) + DURATION ))
while [[ $(date +%s) -lt ${end} ]]; do
  ts=$(date +%s)

  if [[ "${EXTENDED}" == "1" ]]; then
    fmt='{{.Name}},{{.CPUPerc}},{{.MemPerc}},{{.MemUsage}},{{.NetIO}}'
  else
    fmt='{{.Name}},{{.CPUPerc}},{{.MemPerc}},{{.MemUsage}}'
  fi

  if (( ${#CONTAINERS[@]} > 0 )); then
    mapfile -t lines < <(docker stats --no-stream --format "${fmt}" "${CONTAINERS[@]}" 2>/dev/null || true)
  else
    mapfile -t lines < <(docker stats --no-stream --format "${fmt}" 2>/dev/null || true)
  fi

  for line in "${lines[@]}"; do
    if [[ "${EXTENDED}" == "1" ]]; then
      IFS=, read -r name cpu mem_perc mem_usage net_io <<<"${line}"
      IFS=/ read -r mem_used_raw mem_total_raw <<<"${mem_usage}"
      IFS=/ read -r net_rx_raw net_tx_raw <<<"${net_io}"
      mem_used_raw="${mem_used_raw// /}"
      mem_total_raw="${mem_total_raw// /}"
      net_rx_raw="${net_rx_raw// /}"
      net_tx_raw="${net_tx_raw// /}"
      mem_used_b=$(to_bytes "${mem_used_raw}")
      mem_total_b=$(to_bytes "${mem_total_raw}")
      net_rx_b=$(to_bytes "${net_rx_raw}")
      net_tx_b=$(to_bytes "${net_tx_raw}")
      # docker stats can emit all-zero placeholder rows after a container dies.
      if [[ "${mem_total_b}" == "0" && "${mem_used_b}" == "0" && "${net_rx_b}" == "0" && "${net_tx_b}" == "0" ]]; then
        continue
      fi
      echo "${ts},${name},${cpu},${mem_perc},${mem_usage},${net_rx_b},${net_tx_b}"
    else
      IFS=, read -r name cpu mem_perc mem_usage <<<"${line}"
      IFS=/ read -r mem_used_raw mem_total_raw <<<"${mem_usage}"
      mem_used_raw="${mem_used_raw// /}"
      mem_total_raw="${mem_total_raw// /}"
      mem_used_b=$(to_bytes "${mem_used_raw}")
      mem_total_b=$(to_bytes "${mem_total_raw}")
      if [[ "${mem_total_b}" == "0" && "${mem_used_b}" == "0" ]]; then
        continue
      fi
      echo "${ts},${line}"
    fi
  done

  sleep 1
done
EOF
