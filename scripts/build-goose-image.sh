#!/usr/bin/env bash
# Build the standard job sandbox image into the rootless Podman storage.
#
# Set GOOSE_JOB_DOCKERFILE to build a custom image instead. Custom images
# default to extending gaensestall-goose-job:latest.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

STANDARD_DOCKERFILE="sandboxes/standard/Dockerfile"

# Capture explicit command-line overrides before .env is sourced. Compose gives
# the shell environment precedence over .env; build scripts should do the same.
EXPLICIT_IMAGE="${GOOSE_IMAGE:-}"
EXPLICIT_DOCKERFILE="${GOOSE_JOB_DOCKERFILE:-}"
EXPLICIT_EXTRA_PACKAGES="${GOOSE_JOB_EXTRA_PACKAGES:-}"
EXPLICIT_BASE_IMAGE="${GOOSE_JOB_BASE_IMAGE:-}"

# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

IMAGE="${EXPLICIT_IMAGE:-${GOOSE_IMAGE:-gaensestall-goose-job:latest}}"
DOCKERFILE="${EXPLICIT_DOCKERFILE:-${GOOSE_JOB_DOCKERFILE:-$STANDARD_DOCKERFILE}}"
EXTRA_PACKAGES="${EXPLICIT_EXTRA_PACKAGES:-${GOOSE_JOB_EXTRA_PACKAGES:-}}"
BASE_IMAGE="${EXPLICIT_BASE_IMAGE:-${GOOSE_JOB_BASE_IMAGE:-}}"

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required to build the goose job image" >&2
  exit 1
fi

BUILD_ARGS=()
if [ "$DOCKERFILE" != "$STANDARD_DOCKERFILE" ]; then
  BUILD_ARGS+=(--build-arg "BASE_IMAGE=${BASE_IMAGE:-gaensestall-goose-job:latest}")
fi
if [ -n "$EXTRA_PACKAGES" ]; then
  BUILD_ARGS+=(--build-arg "GOOSE_JOB_EXTRA_PACKAGES=${EXTRA_PACKAGES}")
fi

podman build \
  -f "$DOCKERFILE" \
  "${BUILD_ARGS[@]}" \
  -t "$IMAGE" .
podman image inspect "$IMAGE" >/dev/null
echo "Built $IMAGE"
