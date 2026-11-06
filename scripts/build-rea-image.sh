#!/usr/bin/env bash
# Build the reverse-engineering job image (REA + Ghidra) into the rootless
# Podman storage. The normal goose image is built first if it is missing.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

BASE_IMAGE="${GOOSE_JOB_BASE_IMAGE:-kafkallm-goose-job:latest}"
IMAGE="${GOOSE_RE_IMAGE:-kafkallm-rea-job:latest}"

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required to build the REA job image" >&2
  exit 1
fi

if ! podman image exists "$BASE_IMAGE"; then
  echo "Base image $BASE_IMAGE is missing; building it first."
  GOOSE_IMAGE="$BASE_IMAGE" ./scripts/build-goose-image.sh
fi

GOOSE_IMAGE="$IMAGE" \
GOOSE_JOB_BASE_IMAGE="$BASE_IMAGE" \
GOOSE_JOB_DOCKERFILE=goose_job/Dockerfile.rea \
  ./scripts/build-goose-image.sh

echo "Built $IMAGE"
echo "Set GOOSE_RE_IMAGE=$IMAGE for the worker, then submit a job with sandbox=reverse."
