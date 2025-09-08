#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Wrapper to reuse multi-transport fanout with ENGINE=redis
ENGINE=redis "${SCRIPT_DIR}/run_fanout.sh" "$@" || true
