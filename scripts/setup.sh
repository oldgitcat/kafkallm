#!/usr/bin/env bash
# Prepare a local checkout for the first start:
#   - create .env from .env.example if needed
#   - generate a random LLM gateway virtual key
#   - migrate the old OpenRouter-only key to the generic provider setting
#   - verify that the selected provider is usable
#   - render agentgateway/config.generated.yaml
#
# The provider key is never printed. Edit .env yourself; it is gitignored.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh

if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env from .env.example"
fi
chmod 600 .env

python3 - <<'PY'
from pathlib import Path
import secrets
import sys

sys.path.insert(0, "src")
from kafkallm.agentgateway_config import PLACEHOLDER_VIRTUAL_KEYS as placeholder

path = Path(".env")
lines = path.read_text(encoding="utf-8").splitlines()


def value(key: str) -> str:
    prefix = key + "="
    for line in lines:
        if line.startswith(prefix):
            return line.split("=", 1)[1].strip()
    return ""


def has(key: str) -> bool:
    prefix = key + "="
    return any(line.startswith(prefix) for line in lines)


def append(key: str, value_: str) -> None:
    lines.append(f"{key}={value_}")


current = value("LLM_GATEWAY_VIRTUAL_KEY")
if current in placeholder or len(current) < 32:
    generated = secrets.token_hex(32)
    replaced = False
    updated = []
    for line in lines:
        if line.startswith("LLM_GATEWAY_VIRTUAL_KEY="):
            updated.append(f"LLM_GATEWAY_VIRTUAL_KEY={generated}")
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        updated.append(f"LLM_GATEWAY_VIRTUAL_KEY={generated}")
    lines = updated
    print("Generated LLM_GATEWAY_VIRTUAL_KEY")
else:
    print("Kept existing LLM_GATEWAY_VIRTUAL_KEY")

# Provider-neutral settings. Existing values are never overwritten.
if not has("LLM_UPSTREAM_PROVIDER"):
    append("LLM_UPSTREAM_PROVIDER", "openrouter")
if not has("LLM_UPSTREAM_BASE_URL"):
    append("LLM_UPSTREAM_BASE_URL", "")
if not has("LLM_UPSTREAM_FORMAT"):
    append("LLM_UPSTREAM_FORMAT", "completions")
if not has("LLM_UPSTREAM_API_KEY"):
    legacy = value("OPENROUTER_API_KEY")
    if legacy:
        append("LLM_UPSTREAM_API_KEY", legacy)
        print("Migrated OPENROUTER_API_KEY to LLM_UPSTREAM_API_KEY")

# The Compose service was renamed from llm-gateway to agentgateway. Existing
# .env files keep working through a network alias, but migrate the default.
updated = []
for line in lines:
    if line.startswith("LLM_GATEWAY_READY_URL=http://llm-gateway:"):
        updated.append(line.replace("llm-gateway", "agentgateway"))
        print("Updated LLM_GATEWAY_READY_URL to the agentgateway hostname")
    else:
        updated.append(line)
lines = updated

path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

# Ask which LLM provider to use when a terminal is attached. In scripts/CI
# this validates the existing values instead of prompting.
PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
    python3 -m kafkallm.setup_wizard --env-file "$ROOT_DIR/.env"

load_env "$ROOT_DIR"
./scripts/prepare-job-root.sh
./scripts/gui-auth.sh

provider="${LLM_UPSTREAM_PROVIDER:-openrouter}"

./scripts/render-agentgateway-config.sh
echo "Environment is ready ($provider). Next: make doctor"
