#!/bin/sh
# Create the isolated IPv6-only Podman network used by goose jobs.
#
# Rootless Podman creates an IPv4 subnet for every network. On some hosts an
# interface advertises an unusually broad netmask (for example /0 from a mobile
# hotspot), so netavark rejects every IPv4 subnet. The network definition is
# therefore written directly into the rootless Podman storage. Run this as the
# same user that owns the Podman socket:
#
#   ./scripts/init-job-network.sh
#
# The worker only verifies that the network exists. It does not create it,
# because the worker runs in a container and cannot write host Podman storage.
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

PYTHON=python3
if [ -x .venv/bin/python ]; then
    PYTHON=.venv/bin/python
fi

PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}" exec "$PYTHON" -m kafkallm.sandbox.network_cli "$@"
