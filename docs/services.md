# Container and process map

[Back to the main README](../README.md)

This page explains what every long-running Compose service and every
worker-created runtime container does.

## Compose services

| Service | Image | Responsibility | Published / internal ports | Persistent data |
|---|---|---|---|---|
| `kafka` | Apache Kafka 4.4.0 | Queue and compacted state store | `19092` on host, `kafka:9092` in Compose | `kafka-data` volume |
| `kafka-ui` | kafbat/kafka-ui | Inspect topics and messages | <http://localhost:8080> | — |
| `mlflow` | MLflow 3.18 | Trace/observability backend, SQLite + OTLP/HTTP | <http://localhost:5000> | `mlflow-data` volume |
| `rustfs` | RustFS 1.0.0 | S3-compatible input and artifact store | `9000` API, `9001` console (localhost) | `rustfs-data` volume |
| `podman-proxy` | docker-socket-proxy | Filtered rootless Podman API for the worker | `127.0.0.1:2375` inside the worker namespace | Podman socket |
| `agentgateway` | agentgateway 1.7.0 | OpenAI-compatible LLM gateway, provider key, CORS, playground | `8091` LLM, `15000` admin/playground, `15021` readiness | Generated config |
| `agent-worker` | `kafkallm-app` | Consume `jobs.in`, run microVMs, own Podman API, publish results/state/logs/events | — | `/var/lib/kafkallm`, Podman storage |
| `gui` | `kafkallm-app` | Browser UI/API, job submission, agents, sandboxes, live state | <http://localhost:8090> | `/var/lib/kafkallm` |

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
| `jobs.cancel` | delete | Cancellation requests |
| `jobs.logs` | delete | Live activity |
| `agent.events` | delete | Audit events |

See [Jobs and manual Kafka submission](jobs.md) for the exact message format.

### MLflow

MLflow receives OTLP/HTTP traces on `/v1/traces`. Worker and gateway spans share
the job trace: the worker starts it, and the per-job proxy injects that
`traceparent` into every gateway request. See
[Observability with MLflow](observability.md).

### RustFS

The worker and GUI use RustFS as the S3-compatible store for uploaded inputs and
result artifacts. `make up` uses the `storage` profile; without it, jobs can
still run, but uploads and artifact storage are unavailable.

### podman-proxy

The worker must create, start, stop, and remove microVMs through the host's
rootless Podman API. The docker-socket-proxy shares the worker's network
namespace and exposes a filtered API on the worker's loopback. It is equivalent
to root on the host; never publish it or put it on a shared network.

### agentgateway

The gateway holds the real provider key and exposes an OpenAI-compatible API to
jobs. Jobs see only the virtual key. The admin UI also serves the LLM
playground. The gateway is published on port `8091`; the admin/playground UI is
on `15000` (localhost only). See [LLM providers and agentgateway](providers.md).

### agent-worker

The worker is the only Kafka participant that starts jobs. It:

1. Consumes `jobs.in` with manual offset commits.
2. Creates a per-job allowlisting proxy with the job `traceparent`.
3. Creates the main Goose microVM through the Podman API.
4. Collects outputs and publishes `jobs.out`, `jobs.state`, `jobs.logs`, and
   `agent.events`.
5. Optionally creates a separate reflection microVM, validates its skill
   changes, and merges them into the persistent store.
6. Removes the main/reflect microVMs and the per-job proxy.

### GUI

The GUI is stateless: it rebuilds jobs, agents, and sandboxes from compacted
Kafka topics. It submits jobs to `jobs.in`, manages the registries, and streams
live activity. It has no authentication; keep it on localhost or a trusted
network.

### Host CLI

The CLI runs on the host and does not need a Compose container. Kafka is
published on `localhost:19092`; `scripts/kafkallm-cli` creates the local
virtualenv on first use and loads `.env`.

```bash
./scripts/kafkallm-cli submit "Say hello"
./scripts/kafkallm-cli results --from-beginning --idle-timeout 5
```

For uploads, the wrapper points `S3_ENDPOINT_URL` at `http://localhost:9000`
when `.env` uses the Compose hostname `rustfs`. If you use a different endpoint,
override `S3_ENDPOINT_URL` explicitly.

## Worker-created runtime containers

These containers are not Compose services. The worker creates and removes them
through the rootless Podman API.

| Container | Purpose | Network |
|---|---|---|
| `kafkallm-gateway-proxy` | Long-lived global allowlisting proxy / health check | `podman` IPv4 + `kafkallm-jobs` IPv6 |
| `kafkallm-gateway-proxy-<run>` | Per-job proxy; injects `traceparent` and `x-kafkallm-job-id` | `podman` IPv4 + `kafkallm-jobs` IPv6 |
| Main Goose microVM | Runs the job (`goose run`) and writes `/workspace/outputs` | `kafkallm-jobs` only |
| Reflection microVM | Runs the optional learning pass in a fresh workspace | `kafkallm-jobs` only |

The job microVM has no Kafka access, no internet, no LAN access, and no Podman
API. Its only peer is the proxy, which forwards only chat-completions and
model-list requests to the gateway.

## Networks

| Network | Members | Notes |
|---|---|---|
| `kafkallm_default` | Compose services | Kafka, MLflow, RustFS, GUI, worker, gateway |
| `kafkallm-jobs` | Proxies and job microVMs | IPv6-only, internal, no default route |
| `podman` | Proxies | IPv4 route to the gateway on the host |

The worker and `podman-proxy` share a network namespace so the Podman API is
reachable only on the worker's loopback.

## Host paths and volumes

| Path / volume | Contents |
|---|---|
| `/var/lib/kafkallm/jobs` | Per-job workspaces (temporary) |
| `/var/lib/kafkallm/staging` | Upload/download staging |
| `/var/lib/kafkallm/skills` | Persistent skill store |
| `/var/lib/kafkallm/agents` | Agent profile files |
| `/var/lib/kafkallm/tools` / `config` | Sandbox tools and Goose/MCP config |
| `kafka-data` | Kafka log segments |
| `mlflow-data` | MLflow SQLite and artifacts |
| `rustfs-data` | S3 objects |

## Typical data flow

```text
GUI / CLI / Kafka UI
        │
        ▼
    jobs.in ──→ agent-worker ──→ per-job proxy ──→ agentgateway ──→ provider
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
| `8090` | GUI |
| `8080` | Kafka UI |
| `5000` | MLflow |
| `8091` | agentgateway OpenAI-compatible API |
| `15000` | agentgateway admin UI / playground (localhost only) |
| `19000` / `19001` | RustFS API / console |
| `19092` | Kafka from the host |
