#!/usr/bin/env bash
set -euo pipefail

# Collect docker stats from a remote host via SSH and stream to a local CSV file.
# Usage: ./collect_remote_docker_stats.sh <remote_host> <output_csv> <duration_seconds> [ssh_user]

REMOTE_HOST="$1"
OUTPUT_CSV="$2"
DURATION="${3:-60}"
SSH_USER="${4:-ubuntu}"

# Ensure output directory exists
mkdir -p "$(dirname "${OUTPUT_CSV}")"

# Write CSV header
echo "timestamp,container,cpu_perc,mem_perc,mem_usage" > "${OUTPUT_CSV}"

# Command to run on remote host:
# Loop for duration, running docker stats --no-stream --format ...
# We use a small sleep to avoid hammering, but docker stats takes a moment anyway.
REMOTE_CMD="
end=\$(( \$(date +%s) + ${DURATION} ));
while [ \$(date +%s) -lt \$end ]; do
    ts=\$(date +%s);
    docker stats --no-stream --format \"{{.Name}},{{.CPUPerc}},{{.MemPerc}},{{.MemUsage}}\" | \
    while read line; do
        echo \"\$ts,\$line\"
    done;
    sleep 1;
done
"

# Run SSH command and append to local CSV
# We use -o BatchMode=yes to fail fast if auth fails
ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${SSH_USER}@${REMOTE_HOST}" "${REMOTE_CMD}" >> "${OUTPUT_CSV}"
