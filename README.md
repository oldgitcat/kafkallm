# Gänsestall

[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](pyproject.toml)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#project-status)

Gänsestall is a small Apache Kafka job queue where every job is executed by a
disposable [goose](https://github.com/aaif-goose/goose) microVM running the
[goosetown](https://github.com/aaif-goose/goosetown) multi-agent workflow.
Kafka is the queue, the materialized state store, the scheduler, and the audit
log. PostgreSQL backs MLflow tracking and the AI Gateway only.

> [!IMPORTANT]
> Gänsestall is a local development stack, not a hardened multi-tenant service.
> The GUI has no authentication, the internal LLM proxy path speaks plain HTTP,
> and skill updates from the isolated learning pass are validated only structurally.
> The GUI is reachable from the network by default (`GUI_BIND=0.0.0.0`), so run
> the stack only on a trusted network or put an authenticated TLS proxy in
> front of it. See the
> [isolation model](docs/security.md#isolation-model) before exposing it.

## Project status

Gänsestall is pre-1.0 (`0.1.0`). The core job flow and sandbox isolation are
usable, but configuration and APIs may change between minor releases.

## Documentation

- [Setup and configuration](docs/setup.md)
- [LLM providers and the MLflow AI Gateway](docs/providers.md)
- [Architecture and job lifecycle](docs/architecture.md)
- [Jobs and manual Kafka submission](docs/jobs.md)
- [Container and process map](docs/services.md)
- [Agents, skills, and the learning pass](docs/agents.md)
- [Observability with MLflow](docs/observability.md)
- [Operations, testing, and troubleshooting](docs/operations.md)
- [Security and isolation model](docs/security.md)

## Architecture at a glance

```text
Browser / CLI ──→ jobs.in ──→ worker ──→ rootless Podman (krun)
                                  │            │
                                  │            ▼
                                  │      goose microVM
                                  │      ├─ goosetown orchestrator
                                  │      ├─ parallel goose subagents
                                  │      ├─ gtwall coordination
                                  │      └─ /workspace (copy)
                                  │            │
                                  │            ▼
                                  │      LLM proxy ──→ MLflow AI Gateway ──→ LLM provider
                                  │      (MLflow holds the encrypted provider key)
                                  │
                                  ├─→ jobs.out      (results)
                                  ├─→ jobs.state    (compacted GUI state)
                                  ├─→ agents.state  (compacted goosetown flock)
                                  ├─→ sandboxes.state (compacted environments)
                                  ├─→ agent.events  (audit)
                                  ├─→ MLflow        (OTLP traces)
                                  └─→ RustFS        (optional artifacts)

GUI ──→ jobs.in, jobs.state, agents.state, sandboxes.state, agent.events
```

## Cheat sheet

| Task | Command or URL |
|---|---|
| Configure the stack | `make setup` |
| Check the host and services | `make doctor` |
| Build images and start everything | `make bootstrap` |
| Start / stop | `make up` / `make down` |
| Recreate after `.env` changes | `make restart` |
| Follow logs | `make logs` |
| Run tests / lint | `make test` / `make lint` |
| Full local check (lint, tests, docs) | `make check` |
| Delete all Kafka, trace, and storage data | `make reset` |

| Service | URL |
|---|---|
| GUI | `http://<host-ip>:8090` (Caddy basic auth) |
| Kafka UI | <http://localhost:8080> |
| MLflow UI and AI Gateway | <http://localhost:5000> |
| OpenAI-compatible gateway | <http://localhost:5000/gateway/mlflow/v1> |
| RustFS S3 API / console | <http://localhost:19000> / <http://localhost:19001> |

| Host path | Contents |
|---|---|
| `~/.local/share/gaensestall/jobs` | Per-job workspaces |
| `~/.local/share/gaensestall/skills` | Persistent goosetown skills |
| `~/.local/share/gaensestall/agents` | Agent profiles (`*.md`) |
| `~/.local/share/gaensestall/tools`, `config` | Sandbox tools and Goose config |

Kafka is the database: `jobs.state`, `agents.state`, and `sandboxes.state` are
compacted; the other topics expire after `KAFKA_TOPIC_RETENTION_MS`.

## Included

- One-command setup and host diagnostics through `make` and `gaensestall doctor`
- A disposable **goose** microVM per job with the **goosetown** orchestrator,
  worker, writer, reviewer, and local-researcher skills
- GUI-managed goosetown agent profiles stored in compacted Kafka state, with
  “+ New agent” controls and editable display names
- GUI-managed sandbox/environment registry stored in compacted Kafka state, so
  new images appear in the job dropdown without a code change
- The **MLflow AI Gateway** as the only LLM gateway; it stores the provider key
  encrypted in Postgres and exposes an OpenAI-compatible endpoint
- An IPv6-only internal Podman network where the HTTP LLM proxy is the
  only reachable peer; it validates the rotatable proxy token, accepts only the
  OpenAI chat-completions and model-list endpoints, and forwards them to MLflow
- A browser GUI for submitting jobs, uploading input files or binaries,
  watching state, results, and audit events, and downloading result text and
  artifacts; the job list has a text filter and a **Clear finished** action
- An opt-in unlimited job mode without a wall-clock timeout or Goose turn cap;
  oversized results are preserved as downloadable S3 artifacts
- Stop buttons for queued/running jobs, restart for finished jobs, and
  configurable parallel worker slots
- A drop-in agent profile directory (`~/.local/share/gaensestall/agents`) next to the
  persistent skills directory
- A live activity panel with goose thinking, tool calls, tool results, stderr,
  and token usage streamed from the microVM through Kafka
- **MLflow** observability with OTLP traces, token usage, durations, and errors
- An editable sandbox: host-provided tools, Goose config (including MCP extensions), operator-managed
  persistent skills, extra build packages, plus an optional **REA + Ghidra**
  reverse-engineering environment with Kuna, JADX, Frida, apktool, and apksigner
- Job messages that name the sandbox entry and carry its OCI image reference
  (local build or registry tag); the worker pulls the image when it is missing
- A compacted `jobs.state` topic that acts as the GUI database; the GUI rebuilds
  its view from Kafka after a restart
- Compacted `agents.state` and `sandboxes.state` registries
- Apache Kafka topics `jobs.in`, `jobs.out`, `jobs.state`, `agents.state`,
  `sandboxes.state`, and `agent.events`
- A Kafka worker with manual offset commits and a bounded startup wait
- Isolated skill handling: each job edits a disposable copy, and only the
  worker merges structurally validated learning-pass changes into the persistent
  store
- Correlated traces for submission, worker, and the goose run
- Optional S3-compatible RustFS for input files and result artifacts
- A Kafka UI (kafbat/kafka-ui, Apache-2.0) and a CLI for scripting

## Quick start

```bash
make setup       # choose base URL, key, and upstream model
make doctor
make up          # build/start the stack
```

See [docs/setup.md](docs/setup.md) for prerequisites, configuration,
provider examples, and the full command reference.

## How it works

Jobs arrive on Kafka, a worker starts one disposable goose microVM per job,
the job talks only to the LLM proxy and the MLflow AI Gateway, and
results, state, audit events, and traces flow back through Kafka, MLflow, and
RustFS.

See [docs/architecture.md](docs/architecture.md) for the job lifecycle,
[docs/agents.md](docs/agents.md) for the goosetown flock and skills, and
[docs/observability.md](docs/observability.md) for MLflow traces.

## Security

This is a local development stack, not a hardened multi-tenant service.
Read [docs/security.md](docs/security.md) before exposing it.

## Third-party components

The vendored goosetown files under `goose-assets/` are Apache 2.0; see
[goose-assets/LICENSE-goosetown](goose-assets/LICENSE-goosetown). REA, Ghidra, Kuna,
`jadx-headless-mcp`, apktool, and apksigner are installed as separate upstream
components under their own licenses. Frida is installed as a separate
LGPL-2.1 component.
