#!/usr/bin/env bash
set -euo pipefail

export THIS_SHELL_DIR="$(dirname "$0")"

chmod u+x "${THIS_SHELL_DIR}/stop-postgresql.sh"
"${THIS_SHELL_DIR}/stop-postgresql.sh"

chmod u+x "${THIS_SHELL_DIR}/start-postgresql.sh"
"${THIS_SHELL_DIR}/start-postgresql.sh"
