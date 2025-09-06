#!/usr/bin/env bash
set -euo pipefail

# Stop and remove all containers defined in docker-compose.yml
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
ROOT_DIR="${SCRIPT_DIR%/scripts}"
cd "$ROOT_DIR"

echo ">> Stopping and removing compose services"
docker compose down -v
