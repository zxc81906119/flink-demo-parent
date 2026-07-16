#!/usr/bin/env bash
set -euo pipefail

export THIS_SHELL_DIR="$(cd "$(dirname "$0")" && pwd)"

chmod u+x "${THIS_SHELL_DIR}/stop-flink.sh"
"${THIS_SHELL_DIR}/stop-flink.sh"

chmod u+x "${THIS_SHELL_DIR}/stop-hdfs.sh"
"${THIS_SHELL_DIR}/stop-hdfs.sh"

chmod u+x "${THIS_SHELL_DIR}/stop-kafka.sh"
"${THIS_SHELL_DIR}/stop-kafka.sh"

chmod u+x "${THIS_SHELL_DIR}/stop-postgresql.sh"
"${THIS_SHELL_DIR}/stop-postgresql.sh"

chmod u+x "${THIS_SHELL_DIR}/start-hdfs.sh"
"${THIS_SHELL_DIR}/start-hdfs.sh"

chmod u+x "${THIS_SHELL_DIR}/start-flink.sh"
"${THIS_SHELL_DIR}/start-flink.sh"

chmod u+x "${THIS_SHELL_DIR}/start-kafka.sh"
"${THIS_SHELL_DIR}/start-kafka.sh"

chmod u+x "${THIS_SHELL_DIR}/start-postgresql.sh"
"${THIS_SHELL_DIR}/start-postgresql.sh"