#!/usr/bin/env bash
# Create the host directories the worker and GUI mount into their containers.
# This replaces the former kafkallm-job-init Compose service so the stack has
# one less short-lived container.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

JOB_ROOT="${KAFKALLM_JOB_ROOT:-/var/lib/kafkallm}"
if ! mkdir -p \
    "$JOB_ROOT/jobs" \
    "$JOB_ROOT/staging" \
    "$JOB_ROOT/skills" \
    "$JOB_ROOT/agents" \
    "$JOB_ROOT/tools/bin" \
    "$JOB_ROOT/config" 2>/dev/null; then
    cat >&2 <<EOF
Cannot create $JOB_ROOT.

Create it once with your user as owner:
  sudo mkdir -p $JOB_ROOT
  sudo chown -R \$(id -u):\$(id -g) $JOB_ROOT
  ./scripts/prepare-job-root.sh
EOF
    exit 1
fi

# The job microVM writes as a mapped subuid, so the worker and GUI need to be
# able to read and remove everything below the root.
chmod -R a+rwX "$JOB_ROOT" 2>/dev/null || true
echo "Prepared $JOB_ROOT"
