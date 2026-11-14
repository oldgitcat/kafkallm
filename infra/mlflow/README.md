# MLflow AI Gateway

MLflow 3.18 is both the observability backend and the only LLM gateway. The
tracking server exposes an OpenAI-compatible endpoint at
`/gateway/mlflow/v1`, backed by a persistent endpoint definition in Postgres.

- Tracking UI and API: <http://localhost:5000>
- OpenAI-compatible gateway: `http://mlflow:5000/gateway/mlflow/v1`
- Models list: `GET /gateway/mlflow/v1/models`
- Chat: `POST /gateway/mlflow/v1/chat/completions` with `"model":
  "<endpoint-name>"`

## Provisioning

`infra/mlflow/bootstrap_gateway.py` runs before the server starts. It creates or
updates:

1. an encrypted secret with the upstream API key,
2. an `openai` model definition containing the upstream model id and base URL,
3. the gateway endpoint named by `MLFLOW_GATEWAY_ENDPOINT` (default
   `gaensestall`).

The upstream key is encrypted with `MLFLOW_CRYPTO_KEK_PASSPHRASE` in Postgres
and decrypted only by the MLflow process. Jobs only receive
`LLM_PROXY_TOKEN`, which the LLM proxy validates.

## Postgres

MLflow stores experiments, runs, traces, and gateway configuration in
PostgreSQL. The Compose service creates the database and `mlflow db upgrade`
runs automatically on startup. Artifacts remain on the `mlflow-data` volume.

## Self-signed upstreams

`LLM_UPSTREAM_INSECURE_SKIP_TLS_VERIFY=true` disables TLS verification for the
upstream provider inside MLflow. Keep it false for public providers. Prefer a
proper CA bundle for private endpoints when possible.
