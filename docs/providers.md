# LLM endpoint and the MLflow AI Gateway

[Back to the main README](../README.md)

## One OpenAI-compatible endpoint

Gänsestall does not need provider-specific presets. It needs exactly one
**OpenAI-compatible `/v1` endpoint** and one upstream model id:

```dotenv
LLM_UPSTREAM_BASE_URL=https://openrouter.ai/api/v1
LLM_UPSTREAM_API_KEY=...
LLM_UPSTREAM_MODEL=openai/gpt-4.1-mini
```

The endpoint can be:

| Endpoint | `LLM_UPSTREAM_BASE_URL` |
|---|---|
| OpenRouter | `https://openrouter.ai/api/v1` |
| OpenAI | `https://api.openai.com/v1` |
| Ollama on the host | `http://host.containers.internal:11434/v1` |
| Open WebUI | `http://host.containers.internal:3000/api` |
| vLLM / LiteLLM / LM Studio | your OpenAI-compatible `/v1` URL |
| Any self-hosted gateway | your OpenAI-compatible `/v1` URL |

`LLM_UPSTREAM_API_KEY` is optional for local endpoints that do not require
authentication.

## How it works

MLflow 3.18 contains the AI Gateway. The tracking server is also the only LLM
gateway, and it stores the endpoint definition in PostgreSQL.

- `infra/mlflow/bootstrap_gateway.py` runs before the MLflow server starts. It
  creates or updates an encrypted secret, an `openai` model definition, and the
  gateway endpoint named by `MLFLOW_GATEWAY_ENDPOINT` (default `gaensestall`).
- The real provider key is encrypted with `MLFLOW_CRYPTO_KEK_PASSPHRASE` in
  Postgres and decrypted only by MLflow. It is not passed to the worker, the
  GUI, or the job.
- Jobs use `GOOSE_PROVIDER=openai`, `GOOSE_MODEL=gaensestall`, and
  `OPENAI_HOST=http://host.containers.internal:8091`.
- The LLM proxy validates `LLM_PROXY_TOKEN`, rewrites
  `/v1/chat/completions` to `/gateway/mlflow/v1/chat/completions`, and forwards
  to `mlflow:5000` over the Compose network.
- The LLM proxy validates the token; MLflow uses the stored provider key and
  forwards the request to the upstream model. The job network has no route to
  the internet, the LAN, other container networks, or other Podman services.

MLflow's UI at <http://localhost:5000> shows the gateway endpoint, its model,
and the recorded traces. The OpenAI-compatible endpoint is
`http://localhost:5000/gateway/mlflow/v1`.

## Configuration

```dotenv
GOOSE_MODEL=gaensestall
MLFLOW_GATEWAY_ENDPOINT=gaensestall
LLM_UPSTREAM_BASE_URL=https://openrouter.ai/api/v1
LLM_UPSTREAM_MODEL=openai/gpt-4.1-mini
LLM_UPSTREAM_API_KEY=...
```

`GOOSE_MODEL` must match `MLFLOW_GATEWAY_ENDPOINT`. The upstream model id lives
in `LLM_UPSTREAM_MODEL` and is sent to the provider by MLflow.

After changing provider settings:

```bash
make restart
```

The bootstrap updates the encrypted secret and model definition on the next
start.

For a local model server, MLflow's SSRF protection must also permit the scheme
and private address:

```dotenv
MLFLOW_GATEWAY_API_BASE_ALLOWED_SCHEMES=https,http
MLFLOW_GATEWAY_API_BASE_ALLOW_PRIVATE_IPS=true
```

## Self-signed upstream certificates

For a private endpoint with a self-signed certificate, set:

```dotenv
LLM_UPSTREAM_INSECURE_SKIP_TLS_VERIFY=true
```

Then restart:

```bash
make restart
```

This disables upstream certificate verification inside the MLflow process. Do
not enable it for public providers. A proper CA bundle is preferable for
long-lived private endpoints.

## Postgres

MLflow stores experiments, runs, traces, and gateway secrets in PostgreSQL.
The `postgres` Compose service is internal to the stack and is not published on
the host. Artifacts stay on the `mlflow-data` volume.

```dotenv
POSTGRES_USER=mlflow
POSTGRES_PASSWORD=...
POSTGRES_DB=mlflow
MLFLOW_CRYPTO_KEK_PASSPHRASE=...
```

`make setup` generates the database password and the KEK passphrase if they
still contain the example placeholders.
