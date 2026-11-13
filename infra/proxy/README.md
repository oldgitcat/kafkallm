# Proxy

The job network proxy is a separate distroless Python image. It is not part of
the sandbox images.

- Build: `make proxy-image` (or `./scripts/build-proxy-image.sh`)
- Image: `kafkallm-gateway-proxy:latest` (`GOOSE_JOB_PROXY_IMAGE`)
- Runtime: normal container next to the job microVM
- Listener: the same port as `LLM_GATEWAY_PORT` (default `8091`)
- Upstream: agentgateway via `GOOSE_JOB_PROXY_UPSTREAM_HOST/PORT`

The worker starts one global proxy and, when tracing is active, one per-job
proxy carrying the job trace context. The proxy accepts only the Goose
chat-completions and model-list endpoints.
