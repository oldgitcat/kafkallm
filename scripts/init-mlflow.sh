#!/usr/bin/env bash
# Ensure the dedicated agentgateway MLflow experiment exists and record its ID
# in .env. Called by 'make up' before the control-plane services start, and
# available as 'make mlflow-init' for manual reruns.
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. scripts/common.sh
load_env "$ROOT_DIR"

MLFLOW_URI="${MLFLOW_HOST_URI:-http://127.0.0.1:${MLFLOW_UI_PORT:-5000}}"

python3 - "$MLFLOW_URI" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

base = sys.argv[1].rstrip("/")


def request(path: str, payload: dict[str, str] | None = None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    method = "POST" if data is not None else "GET"
    req = urllib.request.Request(
        base + path, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            body = response.read()
            if not body:
                return response.status, {}
            try:
                return response.status, json.loads(body)
            except json.JSONDecodeError:
                return response.status, body.decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except Exception:
        return None, None


for _ in range(180):
    status, _ = request("/health")
    if status == 200:
        break
    time.sleep(1)
else:
    raise SystemExit(f"MLflow did not become ready at {base}")

status, body = request(
    "/api/2.0/mlflow/experiments/get-by-name?experiment_name=agentgateway"
)
if status == 200:
    experiment_id = body["experiment"]["experiment_id"]
else:
    status, body = request(
        "/api/2.0/mlflow/experiments/create", {"name": "agentgateway"}
    )
    if status != 200:
        raise SystemExit(
            f"Could not create the agentgateway experiment: {status} {body}"
        )
    experiment_id = body["experiment_id"]

env_path = Path(".env")
lines = env_path.read_text(encoding="utf-8").splitlines()
key = "MLFLOW_GATEWAY_EXPERIMENT_ID"
updated = []
replaced = False
for line in lines:
    if line.startswith(key + "="):
        updated.append(f"{key}={experiment_id}")
        replaced = True
    else:
        updated.append(line)
if not replaced:
    updated.append(f"{key}={experiment_id}")
env_path.write_text("\n".join(updated) + "\n", encoding="utf-8")
print(f"MLflow gateway experiment: agentgateway (id={experiment_id})")
PY
