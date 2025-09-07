#!/usr/bin/env bash
set -euo pipefail

# Bring up the 3-router star topology only (no clients yet)
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
ROOT_DIR="${SCRIPT_DIR%/scripts}"
cd "$ROOT_DIR"

echo ">> Starting infra (mosquitto, emqx, hivemq, redis, router1, router2, router3)"
docker compose up -d mosquitto emqx hivemq redis router1 router2 router3

echo ">> Done. Check logs: docker compose logs -f --tail=100 mosquitto emqx hivemq redis router1 router2 router3"
