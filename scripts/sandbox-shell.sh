#!/bin/sh
# Open an interactive shell in the exact goose job image, offline.
#
# This is for testing tools, Ghidra, local MCP servers, and skills without
# submitting a job. It has no network access and no host bind mounts: the
# skills, tools, and config are copied into the container before it starts.
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

IMAGE="${GOOSE_IMAGE:-kafkallm-goose-job:latest}"
ROOT="${KAFKALLM_JOB_ROOT:-$HOME/.local/share/kafkallm}"
NAME="kafkallm-sandbox-shell-$$"

cleanup() {
    podman rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

podman create \
    --name "$NAME" \
    --network none \
    -e HOME=/opt/data \
    -e GOOSE_PATH_ROOT=/opt/data/goose \
    -e PATH=/opt/kafkallm/tools/bin:/usr/local/goose/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    -w /workspace \
    --entrypoint /bin/sh \
    "$IMAGE" -c '
        mkdir -p "$GOOSE_PATH_ROOT/config"
        cp -an /opt/kafkallm/config/. "$GOOSE_PATH_ROOT/config/" 2>/dev/null || true
        exec /bin/bash
    ' >/dev/null

if [ -d "$ROOT/skills" ]; then
    tar -C "$ROOT/skills" --transform='s,^\.,.claude/skills,' -cf - . \
        | podman cp - "$NAME:/workspace"
fi
if [ -d "$ROOT/tools" ]; then
    tar -C "$ROOT/tools" --transform='s,^\.,tools,' -cf - . \
        | podman cp - "$NAME:/opt/kafkallm"
fi
if [ -d "$ROOT/config" ]; then
    tar -C "$ROOT/config" --transform='s,^\.,config,' -cf - . \
        | podman cp - "$NAME:/opt/kafkallm"
fi

podman start -ai "$NAME"
