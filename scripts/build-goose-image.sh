#!/usr/bin/env bash
# Build the goose job image into the rootless Podman storage.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${GOOSE_IMAGE:-kafkallm-goose-job:latest}"

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required to build the goose job image" >&2
  exit 1
fi

podman build -f goose_job/Dockerfile -t "$IMAGE" .
podman image inspect "$IMAGE" >/dev/null
echo "Built $IMAGE"
