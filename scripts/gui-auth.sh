#!/usr/bin/env bash
# Configure HTTP basic auth for the GUI behind Caddy. Credentials are stored in
# .env; the password itself is never written there, only its bcrypt hash.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

CADDY_IMAGE="${CADDY_IMAGE:-docker.io/library/caddy:2.10.2-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d}"

valid_hash() {
    case "${1:-}" in
        \$2a\$*|\$2b\$*|\$2y\$*) [ "${#1}" -ge 50 ] ;;
        *) return 1 ;;
    esac
}

if [ -n "${GUI_AUTH_USER:-}" ] && valid_hash "${GUI_AUTH_HASH:-}"; then
    echo "GUI basic auth is already configured for user '$GUI_AUTH_USER'"
    exit 0
fi

if [ ! -t 0 ]; then
    echo "GUI basic auth is not configured." >&2
    echo "Run 'make setup' in a terminal, or set GUI_AUTH_USER and GUI_AUTH_HASH in .env." >&2
    exit 1
fi

default_user="${GUI_AUTH_USER:-admin}"
read -r -p "GUI username [$default_user]: " username
username="${username:-$default_user}"
if [ -z "$username" ]; then
    echo "The GUI username must not be empty." >&2
    exit 1
fi

while true; do
    read -r -s -p "GUI password: " password
    echo
    if [ -z "$password" ]; then
        echo "The GUI password must not be empty." >&2
        continue
    fi
    read -r -s -p "Repeat GUI password: " confirmation
    echo
    if [ "$password" != "$confirmation" ]; then
        echo "Passwords do not match. Try again." >&2
        continue
    fi
    break
done

engine=""
if command -v podman >/dev/null 2>&1; then
    engine=podman
elif command -v docker >/dev/null 2>&1; then
    engine=docker
else
    echo "podman or docker is required to hash the GUI password." >&2
    exit 1
fi

if [ "$engine" = "podman" ]; then
    if ! podman image exists "$CADDY_IMAGE" >/dev/null 2>&1; then
        echo "Pulling Caddy once to generate the password hash..." >&2
        podman pull "$CADDY_IMAGE" >/dev/null
    fi
elif ! docker image inspect "$CADDY_IMAGE" >/dev/null 2>&1; then
    echo "Pulling Caddy once to generate the password hash..." >&2
    docker pull "$CADDY_IMAGE" >/dev/null
fi

hash=$(
    printf '%s\n' "$password" |
        "$engine" run --rm -i "$CADDY_IMAGE" caddy hash-password | tail -n 1
)
if ! valid_hash "$hash"; then
    echo "Caddy did not return a valid bcrypt hash." >&2
    exit 1
fi

python3 - "$username" "$hash" <<'PY'
from pathlib import Path
import sys

username, password_hash = sys.argv[1], sys.argv[2]
path = Path(".env")
lines = path.read_text(encoding="utf-8").splitlines()
values = {
    "GUI_AUTH_USER": username,
    "GUI_AUTH_HASH": f"'{password_hash}'",
}
seen: set[str] = set()
updated: list[str] = []
for line in lines:
    key = line.split("=", 1)[0]
    if key in values:
        updated.append(f"{key}={values.pop(key)}")
        seen.add(key)
    else:
        updated.append(line)
for key, value in values.items():
    updated.append(f"{key}={value}")
path.write_text("\n".join(updated) + "\n", encoding="utf-8")
PY

chmod 600 .env
echo "GUI basic auth configured for user '$username'"
