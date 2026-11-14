#!/bin/sh
# Upgrade the Postgres schema, provision the AI Gateway endpoint, and start the
# MLflow tracking server. The endpoint is ready before Compose reports healthy.
set -eu

: "${MLFLOW_BACKEND_STORE_URI:?MLFLOW_BACKEND_STORE_URI is required}"

echo "Upgrading the MLflow database..."
mlflow db upgrade "$MLFLOW_BACKEND_STORE_URI"

echo "Provisioning the MLflow AI Gateway..."
python3 /opt/gaensestall/bootstrap_gateway.py

echo "Starting the MLflow tracking server..."
exec mlflow server \
    --backend-store-uri "$MLFLOW_BACKEND_STORE_URI" \
    --default-artifact-root "${MLFLOW_ARTIFACT_ROOT:-/mlflow/artifacts}" \
    --host 0.0.0.0 \
    --port 5000 \
    --allowed-hosts "${MLFLOW_ALLOWED_HOSTS:-mlflow,mlflow:5000,localhost,localhost:5000,127.0.0.1,127.0.0.1:5000}"
