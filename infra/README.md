# infra

Configuration and build inputs for the infrastructure services. The actual
Compose service definitions stay in the top-level `compose.yaml`; these folders
group the files and documentation for each component.

| Folder | Component | Purpose |
|---|---|---|
| `agentgateway/` | agentgateway 1.7 | OpenAI-compatible LLM gateway and provider config |
| `caddy/` | Caddy 2.10 | GUI basic auth and reverse proxy |
| `kafka/` | Apache Kafka 4.4 | Job queue, state store, and audit topics |
| `mlflow/` | MLflow 3.18 | Trace/observability backend |
| `s3/` | RustFS 1.0 | S3-compatible upload and artifact storage |
| `proxy/` | distroless Python | Per-job allowlisting proxy for the job network |

The worker, GUI, and sandbox images live in `gaense/` and `stall/`.
