#!/usr/bin/env bash
# Prepare a local checkout for the first start:
#   - create .env from .env.example if needed
#   - generate the LLM proxy token, Postgres password, and MLflow KEK
#   - verify that the configured OpenAI-compatible endpoint is usable
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

sys.path.insert(0, "backend")
from gaensestall.llm.env import (
    DEFAULT_GATEWAY_ENDPOINT,
    PLACEHOLDER_LLM_PROXY_TOKENS as placeholder,
)

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


def set_value(key: str, value_: str) -> None:
    prefix = key + "="
    for index, line in enumerate(lines):
        if line.startswith(prefix):
            lines[index] = f"{key}={value_}"
            return
    append(key, value_)


# Drop settings that belonged to renamed or removed gateway services.
lines = [
    line
    for line in lines
    if not line.startswith(("LLM_GATEWAY_", "GOOSE_JOB_PROXY_", "AGENT_NAME="))
]

current = value("LLM_PROXY_TOKEN")
if current in placeholder or len(current) < 32:
    generated = secrets.token_hex(32)
    replaced = False
    updated = []
    for line in lines:
        if line.startswith("LLM_PROXY_TOKEN="):
            updated.append(f"LLM_PROXY_TOKEN={generated}")
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        updated.append(f"LLM_PROXY_TOKEN={generated}")
    lines = updated
    print("Generated LLM_PROXY_TOKEN")
else:
    print("Kept existing LLM_PROXY_TOKEN")

# One OpenAI-compatible endpoint. Existing values are never overwritten.
if not has("LLM_UPSTREAM_BASE_URL"):
    append("LLM_UPSTREAM_BASE_URL", "https://openrouter.ai/api/v1")

# The job's model id is the MLflow gateway endpoint; the provider model lives
# in LLM_UPSTREAM_MODEL. Migrate an existing upstream GOOSE_MODEL once.
endpoint = value("MLFLOW_GATEWAY_ENDPOINT") or DEFAULT_GATEWAY_ENDPOINT
set_value("MLFLOW_GATEWAY_ENDPOINT", endpoint)
upstream_model = value("LLM_UPSTREAM_MODEL")
old_model = value("GOOSE_MODEL")
if not upstream_model and old_model and old_model != endpoint:
    set_value("LLM_UPSTREAM_MODEL", old_model)
    print("Migrated GOOSE_MODEL to LLM_UPSTREAM_MODEL")
set_value("GOOSE_MODEL", endpoint)

db_placeholders = {"", "gaensestall-mlflow", "mlflow", "change-me"}
if value("POSTGRES_PASSWORD") in db_placeholders:
    set_value("POSTGRES_PASSWORD", secrets.token_hex(16))
    print("Generated POSTGRES_PASSWORD")

kek_placeholders = {"", "gaensestall-local-kek", "change-me", "change-me-mlflow-kek"}
if value("MLFLOW_CRYPTO_KEK_PASSPHRASE") in kek_placeholders:
    set_value("MLFLOW_CRYPTO_KEK_PASSPHRASE", secrets.token_hex(32))
    print("Generated MLFLOW_CRYPTO_KEK_PASSPHRASE")

path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

# Ask for the OpenAI-compatible endpoint when a terminal is attached. In
# scripts/CI this validates the existing values instead of prompting.
PYTHONPATH="$ROOT_DIR/backend${PYTHONPATH:+:$PYTHONPATH}" \
    python3 -m gaensestall.setup.wizard --env-file "$ROOT_DIR/.env"

load_env "$ROOT_DIR"
./scripts/prepare-job-root.sh
./scripts/gui-auth.sh

echo "Environment is ready (upstream: ${LLM_UPSTREAM_BASE_URL:-}). Next: make doctor"
