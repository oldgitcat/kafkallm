# LLM endpoint and agentgateway

[Back to the main README](../README.md)

## One OpenAI-compatible endpoint

Gänsestall does not need provider-specific presets. It needs exactly one
**OpenAI-compatible `/v1` endpoint**:

```dotenv
LLM_UPSTREAM_BASE_URL=https://openrouter.ai/api/v1
LLM_UPSTREAM_API_KEY=...
GOOSE_MODEL=openai/gpt-4.1-mini
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

The committed `infra/agentgateway/config.template.yml` is endpoint-neutral. Its
LLM section is rendered from `.env` into `infra/agentgateway/config.yml`.

- agentgateway talks to the configured OpenAI-compatible endpoint.
- The real provider key exists only in the agentgateway container
  (`LLM_UPSTREAM_API_KEY`). It is not passed to the worker, the GUI, or the job.
- Jobs authenticate to agentgateway with `LLM_GATEWAY_VIRTUAL_KEY`, a static,
  rotatable virtual key.
- The job uses `GOOSE_PROVIDER=openai` and
  `OPENAI_HOST=http://host.containers.internal:8091`. It calls
  `/v1/chat/completions`, which agentgateway forwards upstream.
- agentgateway is the only upstream the job proxy can reach. The job network
  has no route to the internet, the LAN, other container networks, or other
  Podman services.

Render the config after changing `.env`:

```bash
make gateway-config
make restart
```

The admin UI and LLM playground are published on
`http://127.0.0.1:${AGENTGATEWAY_UI_PORT:-15000}`. The playground calls the LLM
listener from the browser, so `AGENTGATEWAY_CORS_ORIGINS` includes the admin UI
origins by default. Add your reverse-proxy origin there if you publish the UI
elsewhere.
