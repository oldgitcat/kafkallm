# infra

Configuration and build inputs for the infrastructure services. The actual
Compose service definitions stay in the top-level `compose.yaml`; these folders
group the files and documentation for each component.

| Folder | Component | Purpose |
|---|---|---|
| `caddy/` | Caddy 2.10 | GUI basic auth and reverse proxy |
| `kafka/` | Apache Kafka 4.4 | Job queue, state store, and audit topics |
| `mlflow/` | MLflow 3.18 + Postgres | Trace backend, AI Gateway, and provider credentials |
| `s3/` | RustFS 1.0 | S3-compatible upload and artifact storage |
| `llm-proxy/` | distroless Python | Per-job LLM proxy for the job network |

The worker, GUI, and sandbox images live in `goose-assets/` and `sandboxes/`.
