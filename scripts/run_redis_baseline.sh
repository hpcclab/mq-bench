#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Wrapper to reuse multi-transport baseline
ENGINE=redis "${SCRIPT_DIR}/run_baseline.sh" "${1:-}" || true
