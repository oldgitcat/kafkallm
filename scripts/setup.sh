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

path = Path(".env")
lines = path.read_text(encoding="utf-8").splitlines()
placeholder = {
    "",
    "change-me-to-a-long-random-string",
    "replace-with-openssl-rand-hex-32",
}


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

path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

# Ask which LLM provider to use when a terminal is attached. In scripts/CI
# this validates the existing values instead of prompting.
PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
    python3 -m kafkallm.setup_wizard --env-file "$ROOT_DIR/.env"

load_env "$ROOT_DIR"

provider="${LLM_UPSTREAM_PROVIDER:-openrouter}"
missing=0
if [ -z "${GOOSE_MODEL:-}" ]; then
    echo "Missing: GOOSE_MODEL (choose a tool-calling model)" >&2
    missing=1
fi

case "$provider" in
    ollama)
        # Ollama does not require an API key. Override LLM_UPSTREAM_BASE_URL
        # if it does not run on the host at the default port.
        if printf '%s' "${GOOSE_MODEL:-}" | grep -q '/'; then
            echo "Warning: GOOSE_MODEL='${GOOSE_MODEL}' looks like a hosted" >&2
            echo "         model. Set it to the Ollama model name (for example" >&2
            echo "         llama3.2) before running a job." >&2
        fi
        ;;
    custom)
        if [ -z "${LLM_UPSTREAM_BASE_URL:-}" ]; then
            echo "Missing: LLM_UPSTREAM_BASE_URL (required for provider=custom)" >&2
            missing=1
        fi
        ;;
    azure|bedrock|vertex|copilot)
        # These providers use cloud or provider-specific authentication. The
        # API key is optional here; set the extra params in the template if
        # your deployment needs them.
        ;;
    *)
        if [ -z "${LLM_UPSTREAM_API_KEY:-${OPENROUTER_API_KEY:-}}" ]; then
            echo "Missing: LLM_UPSTREAM_API_KEY for provider '$provider'" >&2
            case "$provider" in
                openrouter)
                    echo "  Get one at https://openrouter.ai/keys" >&2
                    ;;
                openai)
                    echo "  Get one at https://platform.openai.com/api-keys" >&2
                    ;;
                *)
                    echo "  Set the API key for your provider in .env" >&2
                    ;;
            esac
            missing=1
        fi
        ;;
esac

if [ "$missing" -ne 0 ]; then
    echo >&2
    echo "Edit .env, then run 'make setup' again." >&2
    exit 1
fi

./scripts/render-agentgateway-config.sh
echo "Environment is ready ($provider). Next: make doctor"
