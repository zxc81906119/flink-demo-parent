#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$SCRIPT_DIR/../app"

fail_list=()

for dir in "$APP_DIR"/*/; do
    project=$(basename "$dir")

    if [[ "$project" == *common* || "$project" == "jars" ]]; then
        continue
    fi

    echo "=== Submitting job for: $project ==="
    if ! "$SCRIPT_DIR/job-submit.sh" "$project" 1 ; then
        echo "!!! Failed: $project"
        fail_list+=("$project")
    fi
done

if [[ ${#fail_list[@]} -gt 0 ]]; then
    echo ""
    echo "以下專案提交失敗：${fail_list[*]}"
    exit 1
fi