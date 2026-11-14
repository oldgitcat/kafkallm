# LLM proxy

The job network LLM proxy is a separate distroless Python image. It is not part
of the sandbox images.

- Build: `make llm-proxy-image` (or `./scripts/build-llm-proxy-image.sh`)
- Image: `gaensestall-llm-proxy:latest` (`LLM_PROXY_IMAGE`)
- Runtime: normal container next to the job microVM
- Listener: the same port as `LLM_PROXY_PORT` (default `8091`)
- Upstream: the MLflow AI Gateway via `LLM_PROXY_UPSTREAM_HOST/PORT`
  (`mlflow:5000`)

The worker starts one global proxy and, when tracing is active, one per-job
proxy carrying the job trace context. The proxy validates
`GAENSESTALL_LLM_PROXY_TOKEN`, accepts only the Goose chat-completions and
model-list endpoints, maps them to `/gateway/mlflow/v1/...`, and forwards to
MLflow over the Compose network.
