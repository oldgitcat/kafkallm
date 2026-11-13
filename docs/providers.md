# LLM providers and agentgateway

[Back to the main README](../README.md)

## agentgateway and provider keys

The `agentgateway` Compose service runs
[agentgateway](https://agentgateway.dev/) `v1.7.0`. The committed
`infra/agentgateway/config.template.yml` is provider-neutral; the LLM section is
rendered from `.env` into `infra/agentgateway/config.yml`.

- The real provider key is configured only on agentgateway
  (`LLM_UPSTREAM_API_KEY`). It is not passed to the worker, the job, or the GUI.
- Jobs authenticate to agentgateway with `LLM_GATEWAY_VIRTUAL_KEY`, a static,
  rotatable virtual key. agentgateway validates it, checks model access, and
  replaces it with the provider key before forwarding upstream.
- The job uses `GOOSE_PROVIDER=openai` and
  `OPENAI_HOST=http://host.containers.internal:8091`. goose 1.59 calls
  `/v1/chat/completions`, which agentgateway serves.
- agentgateway is the only upstream the job proxy can reach. The job network
  has no route to the internet, the LAN, other container networks, or other Podman
  services.
- agentgateway stores the provider key only in its process environment and
  never logs it. Rotate the virtual key without rotating the provider key by
  editing `.env` and restarting the worker and gateway.

### Provider examples

Set these values in `.env`, then run `make setup` (or `make up`):

**OpenRouter**
```dotenv
LLM_UPSTREAM_PROVIDER=openrouter
LLM_UPSTREAM_BASE_URL=
LLM_UPSTREAM_API_KEY=sk-or-v1-...
GOOSE_MODEL=openai/gpt-4.1-mini
```

**OpenAI**
```dotenv
LLM_UPSTREAM_PROVIDER=openai
LLM_UPSTREAM_BASE_URL=
LLM_UPSTREAM_API_KEY=sk-...
GOOSE_MODEL=gpt-4.1-mini
```

**Ollama on the host**
```dotenv
LLM_UPSTREAM_PROVIDER=ollama
LLM_UPSTREAM_BASE_URL=http://host.containers.internal:11434/v1
LLM_UPSTREAM_API_KEY=
GOOSE_MODEL=llama3.2
```

**Custom OpenAI-compatible or self-hosted endpoint**
```dotenv
LLM_UPSTREAM_PROVIDER=openai
LLM_UPSTREAM_BASE_URL=https://llm.example.com/v1
LLM_UPSTREAM_API_KEY=...
GOOSE_MODEL=my-model
```

**Custom non-OpenAI wire format**
```dotenv
LLM_UPSTREAM_PROVIDER=custom
LLM_UPSTREAM_FORMAT=completions
LLM_UPSTREAM_BASE_URL=https://llm.example.com/v1
LLM_UPSTREAM_API_KEY=...
GOOSE_MODEL=my-model
```

The admin UI and LLM playground are published on
`http://127.0.0.1:${AGENTGATEWAY_UI_PORT:-15000}`. The playground calls the LLM
listener from the browser, so `AGENTGATEWAY_CORS_ORIGINS` includes the admin UI
origins by default. Add your reverse-proxy origin there if you publish the UI
elsewhere. The playground derives the LLM URL from `LLM_GATEWAY_PORT`, so the
container listener and the host-published port are intentionally the same. The
renderer binds the LLM routes to the `default` gateway on that port instead of
using the deprecated `llm.port` field.

The generated config is mounted read-write so the admin UI can save CORS and
model changes. It contains no provider key; the provider key stays in the
gateway process environment. It does contain the resolved local
`LLM_GATEWAY_VIRTUAL_KEY` by default so the playground's saved-key selection
works. The file is gitignored and local-only. Set
`AGENTGATEWAY_INLINE_VIRTUAL_KEY=false` to keep only the
`$LLM_GATEWAY_VIRTUAL_KEY` reference; the playground then needs the key under
**Raw value**. `make gateway-config` re-renders the file from `.env` and
`infra/agentgateway/config.template.yml`, so put durable settings there; UI-only
changes survive until the next render.

The OpenAI-compatible API is published on
`${LLM_GATEWAY_BIND:-0.0.0.0}:${LLM_GATEWAY_PORT}` because rootless Podman
reaches host services through a non-loopback pasta address. The virtual key is
the access control. The listener is plain HTTP, so keep it on a trusted network
or put it behind a TLS reverse proxy.

Check it with the virtual key:

```bash
curl -fsS \
  -H "Authorization: Bearer $LLM_GATEWAY_VIRTUAL_KEY" \
  http://localhost:8091/v1/models
```

The worker polls agentgateway's readiness endpoint over the Compose network at
`http://agentgateway:15021/healthz/ready`.


