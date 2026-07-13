#!/usr/bin/env bash
set -euo pipefail

export THIS_SHELL_DIR="$(dirname "$0")"

chmod u+x "${THIS_SHELL_DIR}/start-hdfs.sh"
"${THIS_SHELL_DIR}/start-hdfs.sh"

chmod u+x "${THIS_SHELL_DIR}/start-flink.sh"
"${THIS_SHELL_DIR}/start-flink.sh"

chmod u+x "${THIS_SHELL_DIR}/start-kafka.sh"
"${THIS_SHELL_DIR}/start-kafka.sh"

chmod u+x "${THIS_SHELL_DIR}/start-postgresql.sh"
"${THIS_SHELL_DIR}/start-postgresql.sh"

