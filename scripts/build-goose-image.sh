#!/usr/bin/env bash
# Build the goose job image into the rootless Podman storage.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

IMAGE="${GOOSE_IMAGE:-kafkallm-goose-job:latest}"
DOCKERFILE="${GOOSE_JOB_DOCKERFILE:-goose_job/Dockerfile}"
EXTRA_PACKAGES="${GOOSE_JOB_EXTRA_PACKAGES:-}"
BASE_ARGS=()
if [ -n "${GOOSE_JOB_BASE_IMAGE:-}" ]; then
  BASE_ARGS=(--build-arg "BASE_IMAGE=${GOOSE_JOB_BASE_IMAGE}")
fi

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required to build the goose job image" >&2
  exit 1
fi

podman build \
  -f "$DOCKERFILE" \
  --build-arg "GOOSE_JOB_EXTRA_PACKAGES=${EXTRA_PACKAGES}" \
  "${BASE_ARGS[@]}" \
  -t "$IMAGE" .
podman image inspect "$IMAGE" >/dev/null
echo "Built $IMAGE"
