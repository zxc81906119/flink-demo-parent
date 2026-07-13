#!/usr/bin/env bash
set -euo pipefail

for name in "kafka-ui" \
            "kafka" ; do
    if podman container exists "${name}" 2>/dev/null; then
        echo "  -> Stopping ${name}..."
        podman stop "${name}" 2>/dev/null || true
        podman rm -f "${name}" 2>/dev/null || true
    else
        echo "  -> ${name} not found, skipping."
    fi
done

