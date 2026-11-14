#!/usr/bin/env bash
# Build the distroless allowlisting LLM proxy image into the rootless Podman
# storage. This image is separate from the job sandbox images.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

EXPLICIT_IMAGE="${LLM_PROXY_IMAGE:-}"

# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

IMAGE="${EXPLICIT_IMAGE:-${LLM_PROXY_IMAGE:-gaensestall-llm-proxy:latest}}"

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required to build the LLM proxy image" >&2
  exit 1
fi

podman build -f infra/llm-proxy/Dockerfile -t "$IMAGE" .
podman image inspect "$IMAGE" >/dev/null
echo "Built $IMAGE"
