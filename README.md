# Kafkallm

Kafkallm is a small Kafka/Redpanda job queue where every job is executed by a
disposable [goose](https://github.com/aaif-goose/goose) microVM running the
[goosetown](https://github.com/aaif-goose/goosetown) multi-agent workflow.
Kafka is the queue, the materialized state store, the scheduler, and the audit
log. There is no PostgreSQL and no separate scheduler service.

```text
Browser / CLI ──→ jobs.in ──→ worker ──→ rootless Podman (krun)
                                  │            │
                                  │            ▼
                                  │      goose microVM
                                  │      ├─ goosetown orchestrator
                                  │      ├─ parallel goose subagents
                                  │      ├─ gtwall coordination
                                  │      └─ /workspace bind mount
                                  │            │
                                  │            ▼
                                  │      allowlisting proxy ──→ agentgateway ──→ OpenRouter
                                  │      (agentgateway holds the real provider key)
                                  │
                                  ├─→ jobs.out      (results)
                                  ├─→ jobs.state    (compacted GUI state)
                                  ├─→ agent.events  (audit)
                                  ├─→ MLflow        (OTLP traces)
                                  └─→ RustFS        (optional artifacts)

GUI ──→ jobs.in, jobs.state, agent.events
```

## Included

- A disposable **goose** microVM per job with the **goosetown** orchestrator,
  worker, writer, reviewer, and local-researcher skills
- An **[agentgateway](https://agentgateway.dev/)** LLM gateway that holds the
  OpenRouter key and accepts only a rotatable virtual key from jobs
- An IPv6-only internal Podman network where the allowlisting proxy is the only
  reachable peer; it forwards only to agentgateway
- A browser GUI for submitting jobs, uploading input files or binaries, and
  watching state, results, and audit events
- **MLflow** observability with OTLP traces, token usage, durations, and errors
  (Phoenix remains available as an optional second backend)
- An editable sandbox: host-mounted tools, goose/MCP config, persistent skills,
  extra build packages, and an example Ghidra image
- A compacted `jobs.state` topic that acts as the GUI database; the GUI rebuilds
  its view from Kafka after a restart
- Redpanda topics `jobs.in`, `jobs.out`, `jobs.state`, and `agent.events`
- A Kafka worker with manual offset commits and a bounded startup wait
- A persistent skills directory that goose and goosetown improve across jobs
- Correlated traces for submission, worker, and the goose run
- Optional S3-compatible RustFS for input files and result artifacts
- Redpanda Console and a CLI for scripting

## Requirements

- Docker and Docker Compose for the control plane
- KVM (`/dev/kvm`) and Podman with the `krun` runtime (`crun-krun`) for the job
  microVMs
- A rootless Podman API socket:
  `systemctl --user enable --now podman.socket`
- An OpenRouter API key for agentgateway (`OPENROUTER_API_KEY`)
- Network access while building the goose job image (it downloads the pinned
  goose release)

goosetown relies on goose subagents and tool calling. Use a model with
reliable tool calling; small free models may ignore the delegation workflow and
answer directly.

## Quick start

1. Copy the environment template and set a provider:

   ```bash
   cp .env.example .env
   ```

   ```dotenv
   GOOSE_MODEL=nvidia/nemotron-3.5-lightning:free
   LLM_GATEWAY_VIRTUAL_KEY=replace-with-openssl-rand-hex-32
   OPENROUTER_API_KEY=your-openrouter-key
   ```

2. Install the microVM runtime and start the rootless Podman API:

   ```bash
   dnf install crun-krun
   systemctl --user enable --now podman.socket
   ```

   Rootful Podman breaks libkrun's TSI networking on this setup. Use the rootless
   socket.

3. Create the isolated job network. Run this as the Podman user, on the host,
   once per network definition:

   ```bash
   ./scripts/init-job-network.sh
   ```

   The worker only verifies this network; it cannot create it because it runs in
   a container.

4. Build the goose job image into the **Podman** storage. This is separate from
   the Docker build of the worker and GUI:

   ```bash
   ./scripts/build-goose-image.sh
   ```

5. Start the stack:

   ```bash
   docker compose --profile storage up -d --build
   ```

6. Open the GUI and the MLflow UI:

   - GUI: <http://localhost:8090>
   - MLflow traces: <http://localhost:5000>

The GUI is published on `127.0.0.1` only. It submits to `jobs.in` and reads the
state and audit topics. It has no local database.

### Command line

```bash
# Submit a job
docker compose run --rm jobctl submit "Say hello"

# Submit a job with a local file or binary (mounted into the CLI container)
docker compose run --rm -v "$PWD:/data:ro" jobctl submit \
  "Inspect the uploaded binary" --upload /data/tool.bin

# Read results
docker compose run --rm jobctl results --from-beginning --idle-timeout 5

# Read materialized state
docker compose run --rm jobctl states --from-beginning --idle-timeout 5

# Read audit events
docker compose run --rm jobctl events --from-beginning --idle-timeout 5
```

Worker and GUI logs:

```bash
docker compose logs -f agent-worker
docker compose logs -f gui
docker compose logs -f llm-gateway
docker compose logs -f mlflow
```

Redpanda Console runs at <http://localhost:8080>. MLflow runs at
<http://localhost:5000>. Phoenix, if enabled, runs at <http://localhost:6006>.

## How a job runs

1. The GUI or CLI writes a JSON job to `jobs.in` and a `queued` state to
   `jobs.state`.
2. The worker consumes `jobs.in`, publishes `running`, and prepares a unique
   directory under `GOOSE_JOB_WORKSPACE_ROOT`.
3. Files uploaded through the GUI or attached with `submit --upload` are
   downloaded from RustFS into `inputs/`. The job sees them under
   `/workspace/inputs`.
4. The worker ensures the allowlisting proxy is running on the IPv6-only
   `kafkallm-jobs` network, then starts a goose microVM through the rootless
   Podman API. The job is attached only to that internal network, and
   `host.containers.internal` is overridden to the proxy's IPv6 address. The job
   directory is bind mounted at `/workspace`, the prompt at `/jobboot`, and the
   persistent skills at `/workspace/.claude/skills`. The microVM receives
   `OPENAI_HOST` and the gateway virtual key, never the provider key.
5. Inside the microVM, the entrypoint seeds the workspace with goosetown
   (`AGENTS.md`, the `goose` wrapper, `gtwall`, and the role skills) and runs:

   ```bash
   ./goose run --no-session -q --output-format json \
     --with-builtin developer --max-turns "$GOOSE_MAX_TURNS" \
     -i /jobboot/main-prompt.txt
   ```

6. goose talks the OpenAI protocol to `http://host.containers.internal:8091`,
   which resolves to the proxy. The proxy forwards only to agentgateway on the
   host; the job cannot reach the internet, the LAN, Docker Compose services, or
   the Podman API. agentgateway verifies the virtual key, replaces it with the
   OpenRouter key, and streams the upstream response back.
7. The goosetown orchestrator decomposes the job, dispatches parallel subagents
   through goose's `delegate`/`load` tools, coordinates them over `gtwall`, and
   writes result files to `outputs/`.
8. The worker parses the structured goose output, copies `outputs/` back,
   optionally runs a short skill-improvement pass, publishes `jobs.out` and the
   final `jobs.state`, uploads artifacts, and commits the Kafka offset.
9. Audit events go to `agent.events`; traces go to MLflow and, optionally,
   Phoenix.

The microVM never talks to Kafka. The worker is the only Kafka participant.

## agentgateway and provider keys

The `llm-gateway` Compose service runs
[agentgateway](https://agentgateway.dev/) `v1.7.0`. Its config is committed at
`agentgateway/config.yaml`.

- The real OpenRouter key is configured only on agentgateway
  (`OPENROUTER_API_KEY`). It is not passed to the worker, the job, or the GUI.
- Jobs authenticate to agentgateway with `LLM_GATEWAY_VIRTUAL_KEY`, a static,
  rotatable virtual key. agentgateway validates it, checks model access, and
  replaces it with the provider key before forwarding upstream.
- The job uses `GOOSE_PROVIDER=openai` and
  `OPENAI_HOST=http://host.containers.internal:8091`. goose 1.57 calls
  `/v1/chat/completions`, which agentgateway serves.
- agentgateway is the only upstream the job proxy can reach. The job network
  has no route to the internet, the LAN, the Docker networks, or other Podman
  services.
- agentgateway stores the provider key only in its process environment and
  never logs it. Rotate the virtual key without rotating the provider key by
  editing `.env` and restarting the worker and gateway.

The gateway is published on `${LLM_GATEWAY_BIND:-0.0.0.0}:${LLM_GATEWAY_PORT}`
because rootless Podman reaches host services through a non-loopback pasta
address. The virtual key is the access control. The listener is plain HTTP, so
keep it on a trusted network or put it behind a TLS reverse proxy.

Check it with the virtual key:

```bash
curl -fsS \
  -H "Authorization: Bearer $LLM_GATEWAY_VIRTUAL_KEY" \
  http://localhost:8091/v1/models
```

The worker polls agentgateway's readiness endpoint over the Compose network at
`http://llm-gateway:15021/healthz/ready`.

## Kafka topics

| Topic | Cleanup | Purpose |
|---|---|---|
| `jobs.in` | delete, 7 days | Jobs submitted by the GUI or CLI |
| `jobs.out` | delete, 7 days | Final `JobResult` records |
| `jobs.state` | **compact** | Latest materialized `JobState` per `job_id`; the GUI's database |
| `agent.events` | delete, 7 days | Audit events for each job |

The GUI consumes `jobs.state` and `agent.events` from the beginning with a
unique consumer group. Compaction keeps the latest state per job, so a restart
rebuilds the view without a local store. State transitions are published by
both the GUI/CLI (`queued`) and the worker (`running`, `succeeded`/`failed`).

## goosetown and skills

The job image contains a small goosetown hub:

- `AGENTS.md` — workspace and coordination contract for every job
- `goose` — the upstream goosetown launcher (sets telepathy and wall files)
- `gtwall` — the upstream Town Wall broadcast tool
- `.claude/skills/goosetown-orchestrator/SKILL.md` — adapted orchestrator
  guidance for an unattended, offline job
- role skills for workers, writers, reviewers, and a local researcher

The upstream role skills and `gtwall` are vendored from goosetown under
Apache 2.0; see `goose_job/LICENSE-goosetown`. The orchestrator and `AGENTS.md`
are adapted so they do not depend on the internet, `bd`, `uv`, or the goosetown
dashboard. The browser-based goosetown dashboard is not used; Kafkallm has its
own job GUI.

Skills are stored on the host at `GOOSE_JOB_SKILLS_DIR`
(`/var/lib/kafkallm/skills` by default) and bind mounted into every job at
`/workspace/.claude/skills`. A skill improved by one job is available to the
next. The directory is world-writable because rootless Podman maps the job's
uid into a subuid range.

### Learning pass

After a successful main run, the worker can run a second, shorter goose session
with a review prompt. It may update one reusable skill under
`.claude/skills`. This is best-effort: a failed reflect run is reported as a
`goose.reflect` audit event and does not fail the job.

- `GOOSE_JOB_REFLECT_ENABLED=true` (default)
- `GOOSE_JOB_REFLECT_TIMEOUT_SECONDS=300`
- `GOOSE_JOB_MAX_TURNS=60` (main run)
- `GOOSE_JOB_SUBAGENT_MAX_TURNS=40`

## Job format

```json
{
  "job_id": "optionally-set-by-producer",
  "run_id": "optionally-set-by-producer",
  "trace_id": "generated-automatically-via-opentelemetry",
  "type": "llm_task",
  "instruction": "Analyze the input data",
  "input": {"arbitrary": "JSON data"},
  "input_files": [],
  "timeout_seconds": 300,
  "max_steps": 6
}
```

`job_id` and `run_id` are generated as UUIDs if omitted. Messages use `job_id`
as the Kafka key. `max_steps` is advisory; goose is bounded by
`GOOSE_JOB_MAX_TURNS` and the job timeout.

A result includes:

- `status`: `succeeded` or `failed`
- `result` or `error`
- `artifacts`: uploaded files
- `artifact_error`: optional upload failure; does not change job status
- `trace_id`, `worker_id`, timestamps, steps, and token usage

## MicroVM job image

The job image is built with `goose_job/Dockerfile`:

- Debian bookworm slim, with bash, git, jq, Python 3, ripgrep, binutils, file,
  less, unzip/zip, and xxd
- Pinned goose `1.57.0` release, verified with SHA-256 at build time
- No `curl`, `wget`, or other network clients in the final image
- goosetown template and skills under `/opt/goosetown`
- Read-only tools and goose config mounts for sandbox extensions
- Optional extra Debian packages through `GOOSE_JOB_EXTRA_PACKAGES`

Build it with `./scripts/build-goose-image.sh`. The worker expects it in the
rootless Podman storage and verifies it at startup (it can pull a registry
image if `GOOSE_IMAGE` points to one).

Per-job layout:

| Mount | Type | Purpose |
|---|---|---|
| `/workspace` | bind mount `goose-vm-<run_id>` | `inputs/`, `outputs/`, `.scratch/`, and the goosetown workspace; removed after collection |
| `/jobboot` | read-only bind mount | run script, main prompt, reflect prompt |
| `/workspace/.claude/skills` | bind mount `GOOSE_JOB_SKILLS_DIR` | persistent goosetown skills |
| `/opt/data` | tmpfs | goose config, sessions, and wall state; discarded with the microVM |

The microVM costs more than a container: each job reserves `GOOSE_JOB_RAM_MIB`
of memory and boots a guest kernel.

## Editing the sandbox

The job image is intentionally small, but you have four extension points. None
of them weakens the network isolation: the job still reaches only the
allowlisting proxy.

### 1. Host-mounted tools

Put static binaries or extracted tools under `GOOSE_JOB_TOOLS_DIR`
(`/var/lib/kafkallm/tools` by default). The worker mounts this directory
read-only at `/opt/kafkallm/tools` and prepends `/opt/kafkallm/tools/bin` to
`PATH`.

```bash
mkdir -p /var/lib/kafkallm/tools/bin
cp my-analyzer /var/lib/kafkallm/tools/bin/
```

### 2. Goose config and MCP servers

Put a `config.yaml` in `GOOSE_JOB_CONFIG_DIR`
(`/var/lib/kafkallm/config/config.yaml` by default). The worker mounts it
read-only at `/opt/kafkallm/config` and copies it into goose's tmpfs config
directory before each run. See `goose_job/config/config.example.yaml` for a
stdio MCP example.

MCP servers must already exist in the image or in the tools directory. Jobs
have no web access, so remote MCP servers and `npx` downloads do not work.

### 3. Skills

Skills live in `GOOSE_JOB_SKILLS_DIR` (`/var/lib/kafkallm/skills`) and are
bind-mounted read-write at `/workspace/.claude/skills`. Add a directory with a
`SKILL.md`; the next job can use it. The learning pass can also update skills.

### 4. Build a custom image

Install extra Debian packages into the normal image:

```bash
GOOSE_JOB_EXTRA_PACKAGES="openjdk-17-jre-headless" \
  ./scripts/build-goose-image.sh
```

For larger changes, use a custom Dockerfile that extends the job image:

```bash
GOOSE_JOB_DOCKERFILE=goose_job/Dockerfile.example \
  ./scripts/build-goose-image.sh
```

`goose_job/Dockerfile.example` shows how to add Ghidra and Java. Put the Ghidra
release zip in the build context and adjust `GHIDRA_ZIP` if needed.

To inspect the exact sandbox interactively, without a job and without network:

```bash
./scripts/sandbox-shell.sh
```

## Why Docker and Podman

The control plane runs under Docker Compose; the job microVMs run under Podman.

- **Podman runs the microVMs.** Only Podman's `crun` krun runtime can start a
  libkrun microVM, selected through the `run.oci.handler=krun` annotation.
  Docker's runc has no krun handler.
- **Docker runs the control plane.** The worker, GUI, Redpanda, RustFS, MLflow,
  Phoenix (optional), and the socket proxy are ordinary containers.
- **Rootless Podman is required.** Rootful Podman breaks libkrun's TSI
  networking here, so the microVMs would have no outbound network and every job
  would stall on the LLM provider.

The whole stack was also tried under `podman-compose`, but this host's Wi-Fi
interface advertises a `/0` netmask (typical for a mobile hotspot), so netavark
refuses to create any bridge subnet. Docker's bridges already exist, so the
stack stays on Docker while the job microVMs use Podman.

The worker reaches the rootless Podman API through the internal `podman-proxy`,
which shares the worker's network namespace and listens on loopback only. The
job directory and skills live on the host under `/var/lib/kafkallm`, which both
the worker and the microVMs see.

## Isolation model

Each job runs in its own KVM microVM.

| Property | Value |
|---|---|
| Kernel | Its own guest kernel, separate from the host |
| Privilege escalation | `no-new-privileges:true` |
| Resources | `GOOSE_JOB_RAM_MIB` (default `2048`), `GOOSE_JOB_CPUS` (default `2`), `GOOSE_JOB_PIDS_LIMIT` (default `512`) |
| Filesystem | Disposable job directory, tmpfs `/opt/data`, read-only tools/config mounts, writable skills mount |
| Network | Attached only to the IPv6-only `kafkallm-jobs` internal network. The allowlisting proxy is the only peer, and it forwards only to agentgateway. Internet, LAN, Docker Compose services, other Podman services, and the Podman API are unreachable |
| Credentials | The real provider key stays in agentgateway. The job gets a rotatable virtual key that is valid only through the gateway |
| Tools | No browser, no `curl`/`wget` in the image; the prompt forbids web and other network use |
| Lifetime | The microVM and its job directory are removed after the job |

The worker runs with SELinux labeling disabled so it can collect outputs and
remove files that rootless Podman created with private MCS categories. The job
microVM still runs with `no-new-privileges` and the microVM boundary.

What the boundary does **not** cover:

- **The hypervisor.** libkrun and KVM run on the host, so a VMM bug is still a
  host compromise. A microVM raises the bar, but it is not a bulletproof
  boundary.
- **Root-equivalent Podman API.** The socket proxy is equivalent to root on the
  host. It listens on the worker's loopback only, so the job microVMs cannot
  reach it, but a compromised worker still means a compromised host.
- **Shared skills directory.** Every job can write `.claude/skills`. A job can
  poison skills that later jobs load. Use one skills directory per trust domain,
  or run separate stacks.
- **Gateway virtual key.** The job holds a static virtual key for the whole job.
  A malicious job can reuse it for its own gateway requests, and the gateway
  does not enforce a per-job request or token budget. Rotate
  `LLM_GATEWAY_VIRTUAL_KEY` if a job is untrusted.
- **Gateway transport.** The gateway listener is plain HTTP. Keep it on a
  trusted network or terminate TLS in front of it.
- **Proxy configuration.** The proxy forwards every accepted TCP connection to
  one fixed upstream. It has no HTTP awareness, but a compromise of the proxy
  container still gives access to agentgateway.

The `kafkallm-jobs` network is IPv6-only and `internal`, so jobs have no IPv4
address and no default route. The allowlisting proxy is the only other container
on that network. Create the network on the host with
`./scripts/init-job-network.sh`; the worker fails closed if it is missing.

## Observability: MLflow and Phoenix

Every job receives a `trace_id`, propagated through Kafka headers and connected
to the worker and goose spans. Audit events include:

- `job.received` and `job.completed`
- `agent.completed`
- `goose.started` and `goose.completed`
- `goose.reflect`
- `artifact.stored` and `artifact.failed`

**MLflow** is the default backend. It runs as one container with a SQLite store
and an OTLP/HTTP endpoint, so it does not add PostgreSQL, ClickHouse, or Redis.
Open <http://localhost:5000> and use the **Traces** tab. Each trace includes the
job span, the goose microVM span, durations, steps, token usage, artifacts, and
errors.

MLflow receives two kinds of traces:

- **Job traces** from the worker: `job.submit`, `job.handle`, `agent.run`, and
  `goose.job.vm.run`, with steps, token usage, artifacts, and errors.
- **LLM request traces** from agentgateway: request model, status, duration, and
  token usage for every call that passes through the gateway.

MLflow is configured with:

```dotenv
MLFLOW_ENABLED=true
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_OTLP_HOST=mlflow
MLFLOW_OTLP_PORT=5000
MLFLOW_EXPERIMENT_ID=0
```

**Phoenix** remains available as an optional second OTLP backend. Set
`PHOENIX_ENABLED=true` and open <http://localhost:6006>.

Prompts, responses, and tool input/output are not captured by default because
they can contain sensitive data. Set `TELEMETRY_CAPTURE_CONTENT=true` to include
them in the traces. The legacy `PHOENIX_CAPTURE_CONTENT` name is still honoured
as a fallback.

Trace export happens in the worker/control plane, which can reach MLflow over
the Compose network. The goose microVM keeps its strict egress isolation: it
does not export traces directly and reaches only the allowlisting proxy.

## Workspace and artifacts

The worker runs as UID/GID `10001`. It stages each job under its staging
directory and copies inputs into the job directory under
`GOOSE_JOB_WORKSPACE_ROOT`, which is shared between the worker container and the
microVMs. After the job, the worker copies `outputs/` back for upload and
deletes the job directory.

Artifacts are uploaded only from `outputs/`. A missing or unreachable artifact
store does not turn a successful run into a failed job; the job remains
`succeeded` with an `artifact_error`. Uploads are retried three times.

## RustFS and input files

Start with the storage profile:

```bash
docker compose --profile storage up -d --build
```

```dotenv
S3_ENDPOINT_URL=http://rustfs:9000
```

Local RustFS defaults:

- S3 API: <http://localhost:9000> (or `RUSTFS_API_PORT`)
- Console: <http://localhost:9001/rustfs/console/>
- Account: `rustfs`
- Secret: `kafkallm-secret`

These credentials are for local development only.

Submit a job with an existing S3 object:

```bash
docker compose run --rm jobctl submit \
  "Read the input file and summarize it" \
  --input-file jobs/example.txt
```

The bucket must already contain the file.

Upload local files or binaries:

- **GUI:** use **Input files or binaries** in the submit form. The browser
  uploads to `POST /api/uploads`; the files are stored under
  `uploads/<uuid>/<filename>` in RustFS and attached to the job as
  `input_files`. The job copies them into `/workspace/inputs`.
- **CLI:**

  ```bash
  docker compose run --rm -v "$PWD:/data:ro" jobctl submit \
    "Inspect the uploaded files" \
    --upload /data/data.csv \
    --upload /data/tool.bin
  ```

Uploads require `S3_ENDPOINT_URL` and the S3 credentials. Limits are
`UPLOAD_MAX_FILES` (default 16), `UPLOAD_MAX_FILE_BYTES` (default 50 MiB), and
`UPLOAD_MAX_TOTAL_BYTES` (default 100 MiB).

## Scheduling

Kafka is the scheduler. A job becomes runnable when it is written to `jobs.in`;
the `agent-workers` consumer group assigns each partition to one worker, and a
worker takes the next message as soon as it is free. The number of partitions
and workers sets the parallelism. There is no cron or separate scheduler
process. To run a job at a wall-clock time, write it to `jobs.in` at that time
(for example from a cron or systemd timer), or add a delayed producer in front
of the topic.

## Multiple workers

```bash
docker compose up -d --scale agent-worker=2
```

Workers share the `agent-workers` consumer group and process one job each at a
time. The shared skills directory uses last-writer-wins semantics.

## Operations and guarantees

- The worker waits for Redpanda, agentgateway readiness, the Podman API, the
  job network, the allowlisting proxy, and the goose image before consuming
  jobs.
- Redpanda, Console, RustFS, the Podman proxy, MLflow, Phoenix, the worker, and
  the GUI have restart policies.
- The Kafka offset is committed only after the result is written to `jobs.out`.
- Processing is **at-least-once**. A redelivered job starts a new microVM, so
  side effects must be idempotent.
- Audit delivery is asynchronous and best effort and does not change job status.
- `jobs.state` is compacted; the other topics keep seven days by default
  (`KAFKA_TOPIC_RETENTION_MS`).
- The Compose configuration is for local development. Redpanda has no SASL/TLS
  here.
- The GUI has no authentication and is published on `127.0.0.1` only.

Stop without deleting volumes:

```bash
docker compose down
```

## Local tests

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

The unit tests use a fake Podman client, a fake Kafka producer, and the real GUI
HTTP handler. They require neither a Podman daemon nor Redpanda.

## Repository layout

```text
src/kafkallm/       worker, GUI, Kafka, state, goose runtime, models
goose_job/          job image, goosetown template, skills, config and tool examples
scripts/            build-goose-image.sh, init-job-network.sh, sandbox-shell.sh, poc-krun.sh
tests/              unit tests
```
