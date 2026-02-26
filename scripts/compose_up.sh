#!/usr/bin/env bash
set -euo pipefail

# Bring up the infrastructure services
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
ROOT_DIR="${SCRIPT_DIR%/scripts}"
cd "$ROOT_DIR"

echo ">> Starting infra (mosquitto, emqx, hivemq, redis, nats, rabbitmq, artemis, router1)"
docker compose up -d mosquitto emqx hivemq redis nats rabbitmq artemis router1

echo ">> Done. Check logs: docker compose logs -f --tail=100 mosquitto emqx hivemq redis nats rabbitmq artemis router1"
