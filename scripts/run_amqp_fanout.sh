#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE=amqp "${SCRIPT_DIR}/run_fanout.sh" "$@" || true
