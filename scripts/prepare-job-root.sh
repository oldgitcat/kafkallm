#!/usr/bin/env bash
# Create the per-user directories the worker and GUI mount into their
# containers. No sudo is needed: the worker, the GUI, and the microVMs all act
# as the user that runs rootless Podman.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

JOB_ROOT="${GAENSESTALL_JOB_ROOT:-$HOME/.local/share/gaensestall}"
if ! mkdir -p \
    "$JOB_ROOT/jobs" \
    "$JOB_ROOT/staging" \
    "$JOB_ROOT/skills" \
    "$JOB_ROOT/agents" \
    "$JOB_ROOT/tools/bin" \
    "$JOB_ROOT/config"; then
    cat >&2 <<EOF
Cannot create $JOB_ROOT.

Check that your user owns $JOB_ROOT and its parent directories.
EOF
    exit 1
fi

echo "Prepared $JOB_ROOT"
