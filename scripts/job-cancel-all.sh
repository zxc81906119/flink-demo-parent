#!/bin/bash
set -euo pipefail

CONTAINER_PREFIX="flink"
CLIENT_CONTAINER="${CONTAINER_PREFIX}-client"
JOB_MANAGER="jobmanager:8081"  # 若有多套叢集或非預設 REST port，記得調整

echo "=== 取得目前所有 Running Job 清單 ==="
job_list=$(podman exec "${CLIENT_CONTAINER}" flink list -r -m "$JOB_MANAGER" 2>/dev/null | grep "RUNNING" || true)

if [[ -z "$job_list" ]]; then
    echo "目前沒有 Running 中的 Job，結束。"
    exit 0
fi

echo "$job_list"
echo ""

fail_list=()

# flink list 輸出格式範例：
# 16.07.2026 10:00:00 : <job_id> : <job_name> (RUNNING)
while IFS= read -r line; do
    job_id=$(echo "$line" | awk -F ' : ' '{print $2}')
    job_name=$(echo "$line" | awk -F ' : ' '{print $3}' | sed 's/ (RUNNING)//')

    if [[ -z "$job_id" ]]; then
        continue
    fi

    echo "=== Cancelling job: $job_name ($job_id) ==="
    if ! podman exec "${CLIENT_CONTAINER}" flink cancel -m "$JOB_MANAGER" "$job_id"; then
        echo "!!! Failed to cancel: $job_id"
        fail_list+=("$job_id")
    fi
done <<< "$job_list"

if [[ ${#fail_list[@]} -gt 0 ]]; then
    echo ""
    echo "以下 Job 取消失敗：${fail_list[*]}"
    exit 1
fi

echo ""
echo "全部 Job 已取消完成。"