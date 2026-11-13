#!/usr/bin/env bash
# Build the distroless allowlisting proxy image into the rootless Podman
# storage. This image is separate from the job sandbox images.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

EXPLICIT_IMAGE="${GOOSE_JOB_PROXY_IMAGE:-}"

# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

IMAGE="${EXPLICIT_IMAGE:-${GOOSE_JOB_PROXY_IMAGE:-kafkallm-gateway-proxy:latest}}"

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required to build the gateway proxy image" >&2
  exit 1
fi

podman build -f infra/proxy/Dockerfile -t "$IMAGE" .
podman image inspect "$IMAGE" >/dev/null
echo "Built $IMAGE"
