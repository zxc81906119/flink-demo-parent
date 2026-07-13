#!/usr/bin/env bash
set -euo pipefail

CONTAINER_PREFIX="flink"

for name in "${CONTAINER_PREFIX}-client" \
            "${CONTAINER_PREFIX}-taskmanager-2" \
            "${CONTAINER_PREFIX}-taskmanager-1" \
            "${CONTAINER_PREFIX}-jobmanager" ; do
    if podman container exists "${name}" 2>/dev/null; then
        echo "  -> Stopping ${name}..."
        podman stop "${name}" 2>/dev/null || true
        podman rm -f "${name}" 2>/dev/null || true
    else
        echo "  -> ${name} not found, skipping."
    fi
done