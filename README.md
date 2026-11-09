# Kafkallm

[![CI](https://github.com/oldgitcat/kafkallm/actions/workflows/ci.yml/badge.svg)](https://github.com/oldgitcat/kafkallm/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](pyproject.toml)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#project-status)

Kafkallm is a small Apache Kafka job queue where every job is executed by a
disposable [goose](https://github.com/aaif-goose/goose) microVM running the
[goosetown](https://github.com/aaif-goose/goosetown) multi-agent workflow.
Kafka is the queue, the materialized state store, the scheduler, and the audit
log. There is no PostgreSQL and no separate scheduler service.

> [!IMPORTANT]
> Kafkallm is a local development stack, not a hardened multi-tenant service.
> The GUI has no authentication, the LLM gateway speaks plain HTTP, and jobs
> share a writable skills directory. Keep the stack on localhost or a trusted
> network. See [SECURITY.md](SECURITY.md) and the
> [isolation model](#isolation-model) before exposing it.

## Project status

Kafkallm is pre-1.0 (`0.1.0`). The core job flow and sandbox isolation are
usable, but configuration and APIs may change between minor releases.

## Contents

- [Quick start](#quick-start)
- [How a job runs](#how-a-job-runs)
- [Configuration](#configuration)
- [Isolation model](#isolation-model)
- [Operations](#operations-and-guarantees)
- [Development](#local-tests)
- [Troubleshooting](#troubleshooting)
- [License](#license)

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
                                  ├─→ agents.state  (compacted goosetown flock)
                                  ├─→ sandboxes.state (compacted environments)
                                  ├─→ agent.events  (audit)
                                  ├─→ MLflow        (OTLP traces)
                                  └─→ RustFS        (optional artifacts)

GUI ──→ jobs.in, jobs.state, agents.state, sandboxes.state, agent.events
```

## Included

- One-command setup and host diagnostics through `make` and `kafkallm doctor`
- A disposable **goose** microVM per job with the **goosetown** orchestrator,
  worker, writer, reviewer, and local-researcher skills
- GUI-managed goosetown agent profiles stored in compacted Kafka state, with
  “+ New agent” controls and editable display names
- GUI-managed sandbox/environment registry stored in compacted Kafka state, so
  new images appear in the job dropdown without a code change
- An **[agentgateway](https://agentgateway.dev/)** LLM gateway that holds the
  OpenRouter key and accepts only a rotatable virtual key from jobs
- An IPv6-only internal Podman network where the allowlisting proxy is the only
  reachable peer; it forwards only to agentgateway
- A browser GUI for submitting jobs, uploading input files or binaries,
  watching state, results, and audit events, and downloading result text and
  artifacts
- An opt-in unlimited job mode without a wall-clock timeout or Goose turn cap;
  oversized results are preserved as downloadable S3 artifacts
- Stop buttons for queued/running jobs, restart for finished jobs, and
  configurable parallel worker slots
- A drop-in agent profile directory (`/var/lib/kafkallm/agents`) next to the
  persistent skills directory
- A live activity panel with goose thinking, tool calls, tool results, stderr,
  and token usage streamed from the microVM through Kafka
- **MLflow** observability with OTLP traces, token usage, durations, and errors
  (Phoenix remains available as an optional second backend)
- An editable sandbox: host-mounted tools, goose/MCP config, persistent skills,
  extra build packages, plus seeded **Default** and **REA + Ghidra**
  reverse-engineering environments with Kuna, JADX, Frida, apktool, and
  apksigner
- Job messages that name the sandbox entry and carry its OCI image reference
  (local build or registry tag); the worker pulls the image when it is missing
- A compacted `jobs.state` topic that acts as the GUI database; the GUI rebuilds
  its view from Kafka after a restart
- Compacted `agents.state` and `sandboxes.state` registries
- Apache Kafka topics `jobs.in`, `jobs.out`, `jobs.state`, `agents.state`,
  `sandboxes.state`, and `agent.events`
- A Kafka worker with manual offset commits and a bounded startup wait
- A persistent skills directory that goose and goosetown improve across jobs
- Correlated traces for submission, worker, and the goose run
- Optional S3-compatible RustFS for input files and result artifacts
- A Kafka UI (kafbat/kafka-ui, Apache-2.0) and a CLI for scripting

## Quick start

### Prerequisites

- Linux with KVM (`/dev/kvm`) and virtualization enabled in the BIOS
- Docker with the Compose plugin for the control plane
- Rootless Podman with the `krun` runtime (`crun-krun`)
- Python 3.12+ and `make` for the host-side scripts
- An OpenRouter API key
- Network access while building the job images
- About 4 GiB of disk for the default image, more for the optional reverse image

goosetown relies on goose subagents and tool calling. Use a model with reliable
tool calling; small free models may ignore the delegation workflow and answer
directly.

### 1. Configure

```bash
make setup
```

This creates `.env` from `.env.example`, generates
`LLM_GATEWAY_VIRTUAL_KEY`, and checks the required values. If it reports a
missing key, add your `OPENROUTER_API_KEY` to `.env`, choose a `GOOSE_MODEL`,
and run `make setup` again. Then check the host:

```bash
make doctor
```

### 2. Prepare the host

```bash
make network   # once per host: create the isolated IPv6-only Podman network
make images    # build the goose job image into rootless Podman storage
```

If `make doctor` reports a missing Podman socket, enable the rootless API:

```bash
systemctl --user enable --now podman.socket
```

For the optional reverse-engineering sandbox (REA + Ghidra), also run:

```bash
make rea-image
```

### 3. Start the stack

```bash
make up
```

Open:

- GUI: <http://localhost:8090>
- MLflow traces: <http://localhost:5000>
- Kafka UI: <http://localhost:8080>

Once `.env` is valid and the prerequisites are installed, `make bootstrap` runs
setup, network creation, the default image build, and `make up` in one command.

The GUI is published on `127.0.0.1` only. It submits to `jobs.in` and reads the
state and audit topics. It has no local database.

<details>
<summary>Manual setup without make</summary>

The Make targets wrap the scripts in this repository:

```bash
./scripts/setup.sh             # creates .env, generates the gateway key
./scripts/init-job-network.sh
./scripts/build-goose-image.sh
./scripts/build-rea-image.sh   # optional
docker compose --profile storage up -d --build
./scripts/doctor.sh
```

`make help` lists every target.
</details>

### Command line

```bash
# Submit a job
docker compose run --rm jobctl submit "Say hello"

# Submit a job with a local file or binary (mounted into the CLI container)
docker compose run --rm -v "$PWD:/data:ro" jobctl submit \
  "Inspect the uploaded binary" --upload /data/tool.bin

# Submit into the reverse-engineering sandbox (REA + Ghidra)
docker compose run --rm -v "$PWD:/data:ro" jobctl submit \
  "Reverse engineer this binary and summarize its behavior" \
  --sandbox reverse --upload /data/tool.bin

# Pull and run an image that is not in the sandbox registry
docker compose run --rm jobctl submit "Say hello" \
  --sandbox adhoc --image ghcr.io/example/agent:1.2.3

# Submit without the learning pass (skill improvement)
docker compose run --rm jobctl submit "Say hello" --no-reflect

# Submit a complete Job JSON payload from a file or stdin
docker compose run --rm -T jobctl submit --job-json - < job.json

# Print the exact Job JSON schema (job, agent, or sandbox)
docker compose run --rm jobctl schema job

# Read results
docker compose run --rm jobctl results --from-beginning --idle-timeout 5

# Read materialized state
docker compose run --rm jobctl states --from-beginning --idle-timeout 5

# Read audit events
docker compose run --rm jobctl events --from-beginning --idle-timeout 5

# Read live activity (thinking, tool calls, stderr)
docker compose run --rm jobctl logs --from-beginning --idle-timeout 5
```

Worker and GUI logs:

```bash
docker compose logs -f agent-worker
docker compose logs -f gui
docker compose logs -f llm-gateway
docker compose logs -f mlflow
```

The Kafka UI runs at <http://localhost:8080>. MLflow runs at
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
   stdbuf -oL -eL ./goose run --no-session -q --output-format stream-json \
     --with-builtin developer --max-turns "$GOOSE_MAX_TURNS" \
     -i /jobboot/main-prompt.txt
   ```

   In multi-agent mode the worker additionally enables the `summon` and
   `skills` builtins and writes the selected flock to `.claude/agents/*.md`,
   so `delegate(source: "...")` can resolve the role names.

   The worker tails the line-delimited stream-json file and publishes live
   activity to `jobs.logs` for the GUI.

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

## Configuration

All runtime settings live in `.env`, created from `.env.example`. The file is
gitignored. `make setup` generates the virtual gateway key and validates the
required values; `OPENROUTER_API_KEY` is the only required secret.

| Setting | Default | Purpose |
|---|---|---|
| `GOOSE_MODEL` | `nvidia/nemotron-3.5-lightning:free` | Model used inside the job |
| `OPENROUTER_API_KEY` | — | Provider credential; only agentgateway sees it |
| `LLM_GATEWAY_VIRTUAL_KEY` | generated | Rotatable key used by jobs |
| `KAFKALLM_JOB_ROOT` | `/var/lib/kafkallm` | Host directory for workspaces, skills, tools, and config |
| `GOOSE_IMAGE` | `kafkallm-goose-job:latest` | Default job microVM image |
| `GOOSE_RE_IMAGE` | `kafkallm-rea-job:latest` | Reverse-engineering image |
| `GOOSE_JOB_RAM_MIB` / `GOOSE_JOB_CPUS` | `2048` / `2` | Per-job resources |
| `GOOSE_JOB_REFLECT_ENABLED` | `true` | Default for the per-job learning pass |
| `GOOSE_JOB_AGENTS_DIR` | `/var/lib/kafkallm/agents` | Drop-in agent profiles imported at GUI start |
| `GUI_PORT` | `8090` | GUI listener |
| `GUI_MAX_REQUEST_BYTES` | `2097152` | Limit for JSON request bodies (uploads use `UPLOAD_MAX_*`) |
| `LLM_GATEWAY_PORT` | `8091` | LLM gateway listener |
| `S3_ENDPOINT_URL` | `http://rustfs:9000` (storage profile) | Uploads and artifacts |
| `MLFLOW_ENABLED` | `true` | OTLP traces to MLflow |

See `.env.example` for every setting and `agentgateway/config.yaml` for the
gateway policy.

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
| `agents.state` | **compact** | Latest goosetown `AgentSpec` per `agent_id` |
| `sandboxes.state` | **compact** | Latest `SandboxSpec` per `sandbox_id`; the environment registry |
| `jobs.cancel` | delete, 7 days | Cancellation requests published by the GUI |
| `jobs.logs` | delete, 7 days | Live job activity from goose stream-json and stderr |
| `agent.events` | delete, 7 days | Audit events for each job |

The GUI consumes `jobs.state`, `agents.state`, `sandboxes.state`, `jobs.logs`,
and `agent.events` from the beginning with unique consumer groups. Compaction keeps
the latest state per key, so a restart rebuilds the view without a local store.
State transitions are published by both the GUI/CLI (`queued`) and the worker
(`running`, `succeeded`/`failed`).

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

### Agent management

The GUI has a **Goosetown agents** panel. Agent profiles are stored in the
compacted `agents.state` Kafka topic, so there is still no separate database.
On the first GUI start the standard flock is seeded:

- `Orchestrator` — decomposes the job and synthesizes the result
- `Local researcher` — read-only search of inputs, notes, and skills
- `Worker` — builds code, scripts, configs, and data
- `Writer` — synthesizes sources into one deliverable
- `Reviewer` — reviews against explicit acceptance criteria

You can add, edit, enable, or disable agents in the GUI. Each profile has a
name, role, description, instructions, and a list of skills. Use **+ New agent**
next to the flock list or in the panel header to create one; it is selected
automatically, and its display name can be changed later with **Edit**.

The job form has **Use goosetown multi-agent flock**. When it is enabled, the
selected agents are embedded in the job as a snapshot. The worker writes them
to `/jobboot/agents.json` and, more importantly, renders each non-orchestrator
role as an agent source at `.claude/agents/<agent_id>.md`. goose's `summon`
extension discovers those files, so the orchestrator can call
`delegate(source: "<role name>", ...)` and `load(source: ...)`. The run also
enables the `skills` builtin so the role skills are discoverable. When the
flock is disabled, the prompt runs a single goose agent without `summon` or
delegation.

#### Agent profile directory

The GUI also imports agent profiles from `GOOSE_JOB_AGENTS_DIR`
(`/var/lib/kafkallm/agents` by default) at startup, next to the skills
directory. Each `*.md` file needs YAML front matter; the filename is the
`agent_id`:

```markdown
---
name: "Security reviewer"
role: reviewer
description: "Checks security-sensitive outputs."
skills:
  - goosetown-reviewer
enabled: true
---

Review the assigned output for security issues and report file/line references.
```

The body becomes the `instructions` field. The parser accepts the flat subset
used by goose agent sources: strings (optionally quoted), booleans, and lists in
`- item` or `[a, b]` form. Files are imported on every GUI start and win over
the existing registry entry for the same `agent_id`; agents that only exist in
the GUI are untouched. The directory is mounted read-only into the GUI
container, so the GUI never writes it. Kafka remains the runtime registry that
the GUI reads from and edits.

### Sandbox management

The GUI has a **Sandboxes** panel and the job form's sandbox dropdown is built
from it. The registry is the compacted `sandboxes.state` topic, so there is no
separate database. On submit, the GUI writes the selected entry's image
reference into the job as `sandbox_image` (for example
`localhost/kafkallm-goose-job:latest` or `ghcr.io/org/agent:1.2.3`). The worker
runs that reference and pulls it from its registry when it is not already in
local Podman storage. The sandbox entry itself supplies the optional `env`,
`extensions`, and `instructions`. On the first GUI start two environments are
seeded:

- `Default` — `GOOSE_IMAGE`, the normal goose + goosetown image
- `Reverse engineering` — `GOOSE_RE_IMAGE`, with REA as a local MCP server,
  Ghidra, and the RE-specific toolchain

After the first start the registry is the source of truth. Changing
`GOOSE_IMAGE` or `GOOSE_RE_IMAGE` in `.env` does not rewrite existing entries;
edit the image in the GUI instead.

Use **+ New sandbox** to register another image. A sandbox entry has:

| Field | Purpose |
|---|---|
| `name` | Label shown in the dropdown and job detail |
| `image` | OCI image reference, for example `localhost/kafkallm-malware:latest` |
| `description` | Short explanation shown below the dropdown |
| `instructions` | Optional prompt note about the environment's capabilities |
| `env` | Optional non-reserved environment variables for the microVM |
| `extensions` | Optional local goose extensions in `name:/absolute/path` form |
| `enabled` | Disabled environments stay in history but leave the dropdown |

Build or pull the image into the worker's rootless Podman storage first. The
worker verifies the image at job start and gives a clear error if it is
unavailable. `KAFKALLM_*`, `GOOSE_*`, `OPENAI_*`, and process-critical variables
such as `PATH` and `HOME` are reserved and rejected in the GUI, so registry
entries cannot override gateway credentials or runtime controls.

Adding an image therefore needs no Python or Compose change:

1. Build or pull it into the same rootless Podman storage the worker uses
   (`podman build -t localhost/my-agent:latest ...` or
   `podman pull ...`, as the same user that owns `PODMAN_SOCKET`).
2. Open the GUI, choose **Sandboxes → + New sandbox**, and enter the image
   reference plus optional `env` and `extensions`.
3. Submit a small job with that environment. The worker uses the entry's image
   reference and pulls it from the registry if it is not local; watch the live
   activity panel during the first start.

The entry is stored in Kafka and appears immediately in the dropdown. Deleting
an entry disables it for new jobs; queued jobs still carry their image
reference, while `env`, `extensions`, and `instructions` are read from the
registry at run time.

The CLI accepts the same registry IDs:

```bash
docker compose run --rm jobctl submit "Analyze the sample" --sandbox malware-analysis
```

New agent profiles follow the same pattern: **Goosetown agents → + New agent**,
choose a role and skills, then select the agent in the job form. No restart and
no code change are required.

A separate persistent goosetown container is not used: the actual multi-agent
flock runs inside each disposable job microVM. This keeps the jobs isolated and
avoids a second long-lived service that would need host workspace access.

### Learning pass

After a successful main run, the worker can run a second, shorter goose session
with a review prompt. It may update one reusable skill under
`.claude/skills`. This is best-effort: a failed reflect run is reported as a
`goose.reflect` audit event and does not fail the job.

The job form has a **Run learning pass** checkbox. It is checked by default
from the worker setting, and the choice travels in the Job message as
`reflect`. A job can therefore enable or disable the pass regardless of the
worker default. The CLI has `--no-reflect` for the same override.

The pass needs enough remaining time: it is skipped when fewer than 60 seconds
are left at the start, and it reserves at most 25 % of the remaining budget
(capped by `GOOSE_JOB_REFLECT_TIMEOUT_SECONDS`). The main run is shortened so it
cannot consume the reflect budget.

- `GOOSE_JOB_REFLECT_ENABLED=true` (worker default when a job has no `reflect`)
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
  "sandbox": "default",
  "sandbox_image": "kafkallm-goose-job:latest",
  "multi_agent": true,
  "agents": [],
  "unlimited": false,
  "timeout_seconds": 300,
  "max_steps": 6,
  "reflect": true
}
```

`job_id` and `run_id` are generated as UUIDs if omitted. Messages use `job_id`
as the Kafka key. `sandbox` is a registry ID such as `default`, `reverse`, or a
custom environment added in the GUI. `max_steps` is advisory; goose is bounded
by `GOOSE_JOB_MAX_TURNS` and the job timeout.

`sandbox_image` is the OCI reference the worker runs. The GUI fills it from the
selected registry entry; a producer can also submit any pullable reference
directly. The worker checks local Podman storage first and pulls the image from
its registry if it is missing. `env`, `extensions`, and `instructions` still
come from the registry entry named by `sandbox`, and an unknown `sandbox` with
an explicit `sandbox_image` runs with an empty environment. The supplied
`agents` list travels with the job. Older payloads without `sandbox_image`
still work; the worker then uses the image from the registry entry.

### Unlimited jobs

Unlimited jobs are opt-in and must be enabled on both the GUI and worker:

```dotenv
ALLOW_UNLIMITED_JOBS=true
```

Then use **Unlimited runtime and result size** in the GUI, add `--unlimited` to
the CLI, or set `"unlimited": true` in a job document. In that mode:

- the job timeout is ignored
- `GOOSE_MAX_TURNS` and `GOOSE_SUBAGENT_MAX_TURNS` use the much higher
  `UNLIMITED_MAX_TURNS` values (default `100000`)
- the output-file count cap is disabled
- if the result text exceeds `MAX_RESULT_BYTES`, the full text is written to
  `result-full.md` and uploaded as an artifact; the Kafka state gets a bounded
  truncated copy so Kafka cannot reject the message

Kafka still has a transport limit, so a huge result is never placed wholesale
in `jobs.state`. It lives in S3/RustFS and is downloadable from the GUI. Upload
limits (`UPLOAD_MAX_*`) remain separately configurable.

The worker raises `max.poll.interval.ms` to
`UNLIMITED_MAX_POLL_INTERVAL_MS` (default 24 hours, the librdkafka maximum)
when unlimited jobs are enabled. It also polls the consumer while the microVM
runs, so the consumer group membership stays alive during long jobs. An
unlimited job can still occupy a worker and consume provider budget
indefinitely; supervise it or split truly enormous work.

The exact schema is available from the CLI and the HTTP API:

```bash
kafkallm schema job
curl -fsS http://localhost:8090/api/schema/job | jq .
```

`kafkallm submit --job-json job.json` (or `--job-json -` for stdin) accepts the
same complete document, validates it, publishes it to `jobs.in`, and records
the initial `queued` state. The JSON defines the whole job, so do not combine
it with instruction/sandbox flags. `agent_ids` is a GUI-only convenience; raw
job documents must contain the full `agents` list.

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

### Reverse-engineering sandbox

`sandbox=reverse` selects a second image, built from
`goose_job/Dockerfile.rea`, instead of the default image. It adds:

- **REA** `6.8.0` as a local stdio MCP server (`rea mcp`), wired into goose as
  the `rea` extension. The `rea` CLI is also on `PATH`. The MCP input schema
  profile is `compact`; output schemas are set to `off` to keep the tool list
  small (`full` is the other valid output profile).
- **Ghidra** `12.1.4` with **Temurin JDK 21.0.13+9**, the combination verified
  by REA.
- **Kuna** `1.833`, pinned to the release REA verifies, as an alternative
  decompiler provider (`REA_KUNA_COMMAND`). The static musl build avoids glibc
  compatibility warnings on Debian bookworm. Kuna's bundled skill is seeded
  into reverse jobs.
- **JADX** via `jadx-headless-mcp` `0.7.1` (`REA_JADX_MCP_JAR`) for Android
  APK/DEX analysis, plus `apktool` and `apksigner` for packaging and signing.
- **Frida** `17.26.1` with `frida-tools` for local dynamic instrumentation.
  REA's bundled Frida bindings expose the full session/script tool set; the
  standalone `frida`, `frida-ps`, and `frida-trace` CLIs are also on `PATH`.
- Static RE tools: GDB, binutils, binwalk, YARA, strace, ltrace, nasm,
  patchelf, 7-Zip, and Python libraries for Capstone, pyelftools, pefile, and
  pwntools. REA can also use the bundled objdump and GDB.

The reverse image starts from `kafkallm-goose-job:latest`, so build the normal
image first. `scripts/build-rea-image.sh` does that automatically if the base
image is missing. Both images are pinned and verified with SHA-256 at build
time. The reverse image is roughly 4.1 GiB on disk. Verify the REA integration
offline with:

```bash
podman run --rm --network none localhost/kafkallm-rea-job:latest \
  rea doctor --json
```

REA's `doctor` still reports Hopper and IDA as missing because those are
proprietary engines that cannot be redistributed. All open-source providers
shipped in the image are available.

Use it from the GUI (sandbox selector) or the CLI:

```bash
docker compose run --rm -v "$PWD:/data:ro" jobctl submit \
  "Analyze the uploaded binary under /workspace/inputs with REA and explain its network behavior" \
  --sandbox reverse --upload /data/tool.bin
```

The job image is selected from the GUI-managed sandbox registry, not supplied
in the job payload. Job submissions carry only a sandbox ID, and the worker
resolves that ID to the image stored in the registry. The worker still mounts
only `/workspace`, `/jobboot`, the tools/config directories, and the persistent
skills. The microVM still has no internet access; REA and Ghidra run fully
offline against uploaded files. Ghidra is JVM-heavy, so set
`GOOSE_JOB_RAM_MIB=4096` (or more) and raise `GOOSE_JOB_MEMORY_LIMIT` to leave
room for the hypervisor overhead (for example `6g`) when using reverse jobs.
The per-job reservation is the same for all sandboxes, so a larger value
applies to all jobs on that worker.

REA is MIT-licensed. Ghidra and Kuna are Apache 2.0. Frida is LGPL-2.1.
`jadx-headless-mcp` and `apktool`/`apksigner` are Apache 2.0. The image also
installs the Debian packages above under their respective licenses. Frida is
included for local instrumentation; a remote Frida server for physical Android
devices is not bundled.

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

For larger changes, use a custom Dockerfile that extends either job image:

```bash
GOOSE_JOB_BASE_IMAGE=kafkallm-rea-job:latest \
GOOSE_IMAGE=kafkallm-custom-job:latest \
GOOSE_JOB_DOCKERFILE=goose_job/Dockerfile.example \
  ./scripts/build-goose-image.sh
```

`goose_job/Dockerfile.example` shows the pattern. If you extend the reverse
image, it already contains REA, Ghidra, and JDK 21; add only the extra tools
you need. The standard reverse sandbox itself is `goose_job/Dockerfile.rea`.

To inspect the exact sandbox interactively, without a job and without network:

```bash
./scripts/sandbox-shell.sh
```

## Why Docker and Podman

The control plane runs under Docker Compose; the job microVMs run under Podman.

- **Podman runs the microVMs.** Only Podman's `crun` krun runtime can start a
  libkrun microVM, selected through the `run.oci.handler=krun` annotation.
  Docker's runc has no krun handler.
- **Docker runs the control plane.** The worker, GUI, Apache Kafka, RustFS, MLflow,
  Phoenix (optional), and the socket proxy are ordinary containers.
- **Rootless Podman is required.** Rootful Podman can break libkrun's TSI
  networking, leaving the microVMs without an outbound path and stalling every
  job on the LLM provider. Use the rootless socket.

The control plane stays on Docker Compose because some host interfaces expose
unusual netmasks (for example a `/0` netmask from a mobile hotspot) that make
netavark reject every IPv4 bridge subnet. Kafkallm therefore writes an
IPv6-only internal job network directly into rootless Podman's network store
instead of asking netavark to derive one. Docker's existing bridges are
unaffected.

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

### Downloading results and artifacts

The job detail in the GUI has **Download result** for the text result and a
**Download** link for every uploaded artifact. Downloads are proxied through
the GUI, so the browser does not need S3 credentials.

The same files are available over HTTP:

```bash
# Result text as Markdown
curl -fL "http://localhost:8090/api/jobs/<job_id>/result" -o result.md

# Artifact by index, in the same order as the artifacts list
curl -fL "http://localhost:8090/api/jobs/<job_id>/artifacts/0" -o artifact.bin
```

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
  `input_files`. The job copies them into `/workspace/inputs` as
  `NN-<filename>` (for example `00-tool.bin`).
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

### Parallel jobs

`WORKER_CONCURRENCY` runs multiple worker consumers inside one process. Each
slot owns its own Kafka consumer and goose runtime, so jobs execute in
parallel up to `min(WORKER_CONCURRENCY, KAFKA_TOPIC_PARTITIONS)` (three by
default). Set it in `.env`:

```dotenv
WORKER_CONCURRENCY=2
```

You can also add more worker containers with
`docker compose up -d --scale agent-worker=2`. Do not set both higher than the
number of partitions; extra consumers remain idle.

### Stopping jobs

The job detail in the GUI has a **Stop job** button for queued and running
jobs. It publishes a `JobCancel` record to `jobs.cancel` and marks the job as
cancelled in the GUI immediately. Workers consume that topic and:
- stop the active microVM container if the job is running
- skip the job if it has not started yet

The final `JobResult` has `cancelled: true` and a user-facing cancellation
message. Cancellation is best-effort; a job that has just finished may still
publish its normal result.

### Restarting jobs

Finished jobs (`succeeded` or `failed`) have a **Restart job** button. It
queues a new job with a new `job_id` and `run_id`, keeping the instruction,
structured input, input files, sandbox ID, image reference, agent profiles,
limits, and the learning-pass choice. Queued or running jobs cannot be
restarted; stop them first. Clicking an open job row again collapses its detail
view.

### Live activity

The job detail has a **Live activity** panel. The worker tails goose's
`stream-json` output and the stderr files in the job workspace, then publishes
small `JobLog` records to the non-compacted `jobs.logs` topic. The GUI shows:

- assistant text as it streams
- model thinking chunks
- tool calls and tool results
- stderr from the main and reflect runs
- token usage when goose completes

The panel polls the `GET /api/jobs/<job_id>/logs?after=<sequence>` endpoint
every two seconds and keeps a bounded ring buffer per job. Raw log records are
also available with `kafkallm logs`. Job logs expire with the normal topic
retention (`KAFKA_TOPIC_RETENTION_MS`, seven days by default).

## Multiple workers

```bash
docker compose up -d --scale agent-worker=2
```

Workers share the `agent-workers` consumer group. Each worker can run
`WORKER_CONCURRENCY` jobs in parallel, and multiple worker containers add more
slots. The shared skills directory uses last-writer-wins semantics.

## Operations and guarantees

- The worker waits for Kafka, agentgateway readiness, the Podman API, the
  job network, the allowlisting proxy, and the goose image before consuming
  jobs.
- Kafka, the Kafka UI, RustFS, the Podman proxy, MLflow, Phoenix, the worker, and
  the GUI have restart policies.
- The Kafka offset is committed only after the result is written to `jobs.out`.
- Processing is **at-least-once**. A redelivered job starts a new microVM, so
  side effects must be idempotent.
- Audit delivery is asynchronous and best effort and does not change job status.
- `jobs.state` is compacted; the other topics keep seven days by default
  (`KAFKA_TOPIC_RETENTION_MS`).
- The Compose configuration is for local development. Kafka has no SASL/TLS
  here.
- The GUI has no authentication and is published on `127.0.0.1` only.

Stop without deleting volumes:

```bash
docker compose down
```

### Upgrading from Redpanda

Earlier versions used Redpanda. The stack now runs Apache Kafka 4.4 in KRaft
mode (Apache-2.0, no ZooKeeper) and the Apache-2.0 kafbat Kafka UI. The Kafka
protocol, topics, and the Job format are unchanged; only the broker container
and its data volume differ:

- The service names changed (`redpanda` → `kafka`, `console` → `kafka-ui`).
  Recreate the stack with `make down && make up` (the Make targets pass
  `--remove-orphans`) so the old containers release port `19092`.
- The new volume is `kafka-data`, mounted at `/var/lib/kafka/data`.
- Kafka does not read the old `redpanda-data` volume. Start the new stack and
  verify it, then remove the old volume when you no longer need its topics, for
  example `docker volume rm kafkallm_redpanda-data` (adjust the Compose project
  prefix if needed).
- The compacted `agents.state` and `sandboxes.state` topics start empty, so the
  default flock and the two built-in sandboxes are seeded again on the first
  GUI start. Jobs that were only in the old `jobs.in` topic are not migrated.

## Local tests

```bash
make test
make lint
```

`make venv` creates `.venv` and installs the package with its development
dependencies. The equivalent manual commands are:

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
```

The unit tests use a fake Podman client, a fake Kafka producer, and the real GUI
HTTP handler. They require neither a Podman daemon nor Kafka.

## Troubleshooting

Start with:

```bash
make doctor
```

It checks Python, Docker Compose, Podman, the rootless socket, KVM, the job
network, the job image, the required `.env` values, the gateway, the GUI, and
OpenRouter connectivity. Use `./scripts/doctor.sh --offline` to skip network
checks.

Common failures:

| Symptom | Fix |
|---|---|
| `KVM` fails | Enable virtualization in the BIOS and make `/dev/kvm` writable |
| `Podman socket` fails | `systemctl --user enable --now podman.socket` |
| `Job network` warns | Run `make network` on the host, not inside a container |
| `Goose job image` warns | Run `make images` |
| Worker waits for the gateway | Check `OPENROUTER_API_KEY` in `.env`, then `make restart` |
| Gateway rejects the virtual key | Rerun `make setup`, then `make restart` |
| `docker compose config` fails | Fix the reported `.env` or `compose.yaml` value |

Logs:

```bash
make logs
docker compose logs -f agent-worker
docker compose logs -f gui
docker compose logs -f llm-gateway
```

## Repository layout

```text
src/kafkallm/       worker, GUI, job store, doctor, Kafka, state, agents,
                    sandboxes, goose runtime, output parsing, models
goose_job/          job image, goosetown template, skills, config and tool examples
agentgateway/       LLM gateway policy
scripts/            setup, doctor, image builds, network init, sandbox helpers
tests/              unit tests
.github/workflows/  CI (lint, tests, Compose validation)
Makefile            one-command setup, diagnostics, build, and run targets
```

## Contributing

Issues and pull requests are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md)
for the development setup and review checklist. Security issues should be
reported privately as described in [SECURITY.md](SECURITY.md).

## License

Kafkallm is licensed under the [Apache License 2.0](LICENSE). The vendored
goosetown files under `goose_job/` are also Apache 2.0; see
[goose_job/LICENSE-goosetown](goose_job/LICENSE-goosetown). REA, Ghidra, Kuna,
`jadx-headless-mcp`, apktool, and apksigner are installed as separate upstream
components under their own licenses. Frida is installed as a separate
LGPL-2.1 component.
