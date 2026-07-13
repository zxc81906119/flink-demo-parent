#!/usr/bin/env bash
set -euo pipefail

export THIS_SHELL_DIR="$(dirname "$0")"

chmod u+x "${THIS_SHELL_DIR}/stop-flink.sh"
"${THIS_SHELL_DIR}/stop-kafka.sh"

chmod u+x "${THIS_SHELL_DIR}/start-flink.sh"
"${THIS_SHELL_DIR}/start-kafka.sh"
