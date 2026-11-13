#!/usr/bin/env bash
# Render infra/agentgateway/config.yml from the provider settings in .env.
# The provider API key is not written to the file; agentgateway reads it from
# LLM_UPSTREAM_API_KEY in its own environment.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
    exec python3 -m kafkallm.agentgateway_config "$@"
