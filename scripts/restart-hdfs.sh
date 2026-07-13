#!/usr/bin/env bash
set -euo pipefail

export THIS_SHELL_DIR="$(dirname "$0")"

chmod u+x "${THIS_SHELL_DIR}/stop-hdfs.sh"
"${THIS_SHELL_DIR}/stop-hdfs.sh"

chmod u+x "${THIS_SHELL_DIR}/start-hdfs.sh"
"${THIS_SHELL_DIR}/start-hdfs.sh"
