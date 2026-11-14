# Container and process map

[Back to the main README](../README.md)

This page explains what every long-running Compose service and every
worker-created runtime container does.

## Compose services

| Service | Image | Responsibility | Published / internal ports | Persistent data |
|---|---|---|---|---|
| `kafka` | Apache Kafka 4.4.0 | Queue and compacted state store | `19092` on host, `kafka:9092` in Compose | `kafka-data` volume |
| `kafka-ui` | kafbat/kafka-ui | Inspect topics and messages | <http://localhost:8080> | — |
| `postgres` | PostgreSQL 17 | Durable MLflow tracking and gateway store | internal `5432` | `postgres-data` volume |
| `mlflow` | MLflow 3.18 | Trace backend, AI Gateway, provider key, OTLP/HTTP | <http://localhost:5000> | `mlflow-data` + `postgres-data` volumes |
| `rustfs` | RustFS 1.0.0 | S3-compatible input and artifact store | `9000` API, `9001` console (localhost) | `rustfs-data` volume |
| `worker` | `gaensestall-worker` | Consume `jobs.in`, run microVMs, own Podman API, publish results/state/logs/events | — | `~/.local/share/gaensestall`, Podman storage |
| `gui` | `gaensestall-gui` | Browser UI/API, job submission, agents, sandboxes, live state | internal `8090` | `~/.local/share/gaensestall` |
| `caddy` | Caddy 2.10 | HTTP basic auth and reverse proxy in front of the GUI | `GUI_BIND:8090` (default all interfaces) | — |

`rustfs` is behind the optional `storage` profile; `make up` enables it. The GUI
and worker are always present in the stack. Host directories are created by
`scripts/prepare-job-root.sh`, which `scripts/setup.sh` and `make up` call.

### Kafka

Kafka is both the queue and the GUI database. The important topics are:

| Topic | Cleanup | Purpose |
|---|---|---|
| `jobs.in` | delete | Submitted jobs |
| `jobs.out` | delete | Final `JobResult` |
| `jobs.state` | compact | Latest `JobState` per job |
| `agents.state` | compact | Agent registry |
| `sandboxes.state` | compact | Sandbox registry |
| `jobs.control` | delete | Cancel and retry requests |
| `jobs.logs` | delete | Live activity |
| `agent.events` | delete | Audit events |

See [Jobs and manual Kafka submission](jobs.md) for the exact message format.

### MLflow and Postgres

MLflow receives OTLP/HTTP traces on `/v1/traces` and serves the AI Gateway at
`/gateway/mlflow/v1`. Worker and gateway spans share the job trace: the worker
starts it, and the per-job LLM proxy injects that `traceparent` into every gateway
request. See [Observability with MLflow](observability.md) and
[LLM providers and the MLflow AI Gateway](providers.md).

PostgreSQL stores experiments, runs, traces, and encrypted LLM proxy credentials.
It is internal to the Compose network; only MLflow talks to it.

### RustFS

The worker and GUI use RustFS as the S3-compatible store for uploaded inputs and
result artifacts. `make up` uses the `storage` profile; without it, jobs can
still run, but uploads and artifact storage are unavailable.

### Podman socket

The worker creates, starts, stops, and removes microVMs through the host's
rootless Podman API. Compose mounts the Unix socket directly into the worker
(`PODMAN_SOCKET`); there is no TCP socket proxy. Access to that socket is
equivalent to root on the host, so it is never published on a network.

### Worker

The worker is the only Kafka participant that starts jobs. It:

1. Consumes `jobs.in` with manual offset commits.
2. Creates a per-job LLM proxy with the job `traceparent`.
3. Creates the main Goose microVM through the Podman API.
4. Collects outputs and publishes `jobs.out`, `jobs.state`, `jobs.logs`, and
   `agent.events`.
5. Optionally creates a separate reflection microVM, validates its skill
   changes, and merges them into the persistent store.
6. Removes the main/reflect microVMs and the per-job LLM proxy.

### GUI

The GUI is stateless: it rebuilds jobs, agents, and sandboxes from compacted
Kafka topics. It submits jobs to `jobs.in`, manages the registries, and streams
live activity. Caddy is the only published GUI entry point and enforces HTTP
basic auth; the GUI itself listens only on the Compose network.

### Host CLI

The CLI runs on the host and does not need a Compose container. Kafka is
published on `127.0.0.1:19092`; `scripts/gaensestall-cli` creates the local
virtualenv on first use and loads `.env`.

```bash
./scripts/gaensestall-cli submit "Say hello"
./scripts/gaensestall-cli results --from-beginning --idle-timeout 5
```

For uploads, the wrapper points `S3_ENDPOINT_URL` at `http://localhost:9000`
when `.env` uses the Compose hostname `rustfs`. If you use a different endpoint,
override `S3_ENDPOINT_URL` explicitly.

## Worker-created runtime containers

These containers are not Compose services. The worker creates and removes them
through the rootless Podman API.

| Container | Purpose | Network |
|---|---|---|
| `gaensestall-llm-proxy` | Long-lived global LLM proxy / health check (distroless Python) | `gaensestall` IPv4 + `gaensestall-jobs` IPv6 |
| `gaensestall-llm-proxy-<run>` | Per-job LLM proxy; validates the proxy token and injects `traceparent` / `x-gaensestall-job-id` (distroless Python) | `gaensestall` IPv4 + `gaensestall-jobs` IPv6 |
| Main Goose microVM | Runs the job (`goose run`) and writes `/workspace/outputs` | `gaensestall-jobs` only |
| Reflection microVM | Runs the optional learning pass in a fresh workspace | `gaensestall-jobs` only |

The job microVM has no Kafka access, no internet, no LAN access, and no Podman
API. Its only peer is the LLM proxy, which forwards only chat-completions and
model-list requests to the gateway.

## Networks

| Network | Members | Notes |
|---|---|---|
| `gaensestall` | Compose services and proxies | Kafka, Postgres, MLflow, RustFS, GUI, worker, proxy |
| `gaensestall-jobs` | Proxies and job microVMs | IPv6-only, internal, no default route |

The worker mounts the rootless Podman socket directly; no extra network
namespace or proxy container is involved.

## Host paths and volumes

| Path / volume | Contents |
|---|---|
| `~/.local/share/gaensestall/jobs` | Per-job workspaces (temporary) |
| `~/.local/share/gaensestall/staging` | Upload/download staging |
| `~/.local/share/gaensestall/skills` | Persistent skill store |
| `~/.local/share/gaensestall/agents` | Agent profile files |
| `~/.local/share/gaensestall/tools` / `config` | Sandbox tools and Goose config |
| `kafka-data` | Kafka log segments |
| `mlflow-data` | MLflow artifacts |
| `postgres-data` | MLflow tracking database and encrypted gateway secrets |
| `rustfs-data` | S3 objects |

## Typical data flow

```text
GUI / CLI / Kafka UI
        │
        ▼
    jobs.in ──→ worker ──→ per-job LLM proxy ──→ MLflow AI Gateway ──→ provider
        │              │
        │              ├──→ main/reflect microVMs
        │              ├──→ jobs.out     (result)
        │              ├──→ jobs.state   (GUI database)
        │              ├──→ jobs.logs    (live activity)
        │              ├──→ agent.events (audit)
        │              ├──→ MLflow       (traces)
        │              └──→ RustFS       (artifacts)
        │
        └──→ GUI replays jobs.state/jobs.logs/agent.events
```

## Ports

| Port | Service |
|---|---|
| `8090` | Caddy → GUI (basic auth) |
| `8080` | Kafka UI |
| `5000` | MLflow UI, tracking, and OpenAI-compatible AI Gateway |
| `19000` / `19001` | RustFS API / console |
| `19092` | Kafka from the host |
