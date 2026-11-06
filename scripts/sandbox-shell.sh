#!/bin/sh
# Open an interactive shell in the exact goose job image, offline.
#
# This is for testing tools, Ghidra, local MCP servers, and skills without
# submitting a job. It has no network access.
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

IMAGE="${GOOSE_IMAGE:-kafkallm-goose-job:latest}"
ROOT="${KAFKALLM_JOB_ROOT:-/var/lib/kafkallm}"

exec podman run --rm -it \
    --network none \
    --tmpfs /opt/data:rw,size=1g \
    -e HOME=/opt/data \
    -e GOOSE_PATH_ROOT=/opt/data/goose \
    -e PATH=/opt/kafkallm/tools/bin:/usr/local/goose/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    -v "$ROOT/skills:/workspace/.claude/skills:rw,z" \
    -v "$ROOT/tools:/opt/kafkallm/tools:ro,z" \
    -v "$ROOT/config:/opt/kafkallm/config:ro,z" \
    -w /workspace \
    --entrypoint /bin/sh \
    "$IMAGE" -c '
        mkdir -p "$GOOSE_PATH_ROOT/config"
        cp -an /opt/kafkallm/config/. "$GOOSE_PATH_ROOT/config/" 2>/dev/null || true
        exec /bin/bash
    '
