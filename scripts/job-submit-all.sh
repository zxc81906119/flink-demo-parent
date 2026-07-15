#!/usr/bin/env bash
set -euo pipefail

export THIS_SHELL_DIR="$(dirname "$0")"

chmod u+x "${THIS_SHELL_DIR}/job-submit.sh"
# 提交 demo1 和 demo2 job , task 並行度設置為 1
"${THIS_SHELL_DIR}/job-submit.sh" "flink-demo-1" 1
"${THIS_SHELL_DIR}/job-submit.sh" "flink-demo-2" 1
