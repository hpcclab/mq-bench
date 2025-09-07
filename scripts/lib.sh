#!/usr/bin/env bash
set -euo pipefail

# Helper: default VAR to value if unset/empty
def() { local var="$1" val="$2"; eval "[ -n \"\${$var-}\" ] || $var=\"$val\""; }

# Helper: print a header
hdr() { echo "==== $* ===="; }

