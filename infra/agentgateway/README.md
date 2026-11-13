# Agentgateway

agentgateway 1.7 is the OpenAI-compatible LLM gateway. The service is defined
in the top-level `compose.yaml`.

- LLM listener: <http://localhost:8091/v1>
- Admin UI / playground: <http://localhost:15000>
- Readiness: `:15021/healthz/ready`
- Config template: `infra/agentgateway/config.template.yml`
- Generated config: `infra/agentgateway/config.yml` (gitignored)

The provider key stays in agentgateway. Jobs receive only the rotatable virtual
key. Render the config from `.env` with:

```bash
make gateway-config
```

See `docs/providers.md` for provider selection and custom endpoints.
