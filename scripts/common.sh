#!/bin/sh
# Shared helpers for the host-side scripts. Source this file, then call
# load_env "$ROOT_DIR" so every script sees the same .env values as Compose.

load_env() {
    env_root=$1
    if [ -f "$env_root/.env" ]; then
        set -a
        # shellcheck disable=SC1090
        . "$env_root/.env"
        set +a
    fi
}
