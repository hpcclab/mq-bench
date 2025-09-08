#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Wrapper to maintain back-compat; delegates to run_baseline.sh with ENGINE=zenoh
ENGINE=zenoh "${SCRIPT_DIR}/run_baseline.sh" "${1:-}" || true