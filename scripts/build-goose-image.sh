#!/usr/bin/env bash
# Build the goose job image into the rootless Podman storage.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${GOOSE_IMAGE:-kafkallm-goose-job:latest}"
DOCKERFILE="${GOOSE_JOB_DOCKERFILE:-goose_job/Dockerfile}"
EXTRA_PACKAGES="${GOOSE_JOB_EXTRA_PACKAGES:-}"

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required to build the goose job image" >&2
  exit 1
fi

podman build \
  -f "$DOCKERFILE" \
  --build-arg "GOOSE_JOB_EXTRA_PACKAGES=${EXTRA_PACKAGES}" \
  -t "$IMAGE" .
podman image inspect "$IMAGE" >/dev/null
echo "Built $IMAGE"
