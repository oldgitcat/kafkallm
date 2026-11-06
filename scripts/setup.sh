#!/usr/bin/env bash
# Prepare a local checkout for the first start:
#   - create .env from .env.example if needed
#   - generate a random LLM gateway virtual key
#   - verify that the required provider settings are present
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

current = ""
for line in lines:
    if line.startswith("LLM_GATEWAY_VIRTUAL_KEY="):
        current = line.split("=", 1)[1].strip()
        break

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
    path.write_text("\n".join(updated) + "\n", encoding="utf-8")
    print("Generated LLM_GATEWAY_VIRTUAL_KEY")
else:
    print("Kept existing LLM_GATEWAY_VIRTUAL_KEY")
PY

load_env "$ROOT_DIR"

missing=0
if [ -z "${OPENROUTER_API_KEY:-}" ]; then
    echo "Missing: OPENROUTER_API_KEY (get one at https://openrouter.ai/keys)" >&2
    missing=1
fi
if [ -z "${GOOSE_MODEL:-}" ]; then
    echo "Missing: GOOSE_MODEL (choose a tool-calling model)" >&2
    missing=1
fi

if [ "$missing" -ne 0 ]; then
    echo >&2
    echo "Edit .env, then run 'make setup' again." >&2
    exit 1
fi

echo "Environment is ready. Next: make doctor"
