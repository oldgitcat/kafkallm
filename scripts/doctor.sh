#!/usr/bin/env bash
# Check host prerequisites, configuration, and running services.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

PYTHON=python3
if [ -x .venv/bin/python ]; then
    PYTHON=.venv/bin/python
fi

PYTHONPATH="backend${PYTHONPATH:+:$PYTHONPATH}" exec "$PYTHON" -m gaensestall.setup.doctor "$@"
