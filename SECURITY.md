# Security policy

## Supported versions

Gänsestall is pre-1.0 software. Security fixes are applied to the `main` branch;
there are no maintained release branches yet.

## Reporting a vulnerability

Please report security issues privately through the repository's **Security**
tab using a [private vulnerability report](https://github.com/oldgitcat/kafkallm/security/advisories/new).
Do not open a public issue for a vulnerability.

Include the affected version or commit, reproduction steps, and the impact you
expect. We will acknowledge the report and coordinate a fix and disclosure.

## Deployment assumptions

Gänsestall is a local development stack, not a hardened multi-tenant service.
Before exposing it to anyone else:

- Keep the GUI on `127.0.0.1`. It has no authentication or authorization.
- Treat `LLM_GATEWAY_VIRTUAL_KEY` as a secret. Rotate it when a job or worker is
  untrusted.
- Keep the agentgateway listener on a trusted network. It speaks plain HTTP.
- Never commit `.env`. Only `.env.example` belongs in Git.
- Treat the persistent skill store as untrusted input. The main job cannot write
  it, but the isolated learning pass can propose changes that later jobs load.
  Merge validation is structural, not semantic; review learned skills before
  trusting them. Use separate skill directories per trust domain if needed.
- The rootless Podman socket proxy is equivalent to root on the host. Do not
  publish it or place it on a shared network.

The full isolation model and its limitations are documented in the main README.
