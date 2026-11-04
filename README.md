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
                                  │      LLM gateway  ──→ model provider
                                  │      (real provider key)
                                  │
                                  ├─→ jobs.out      (results)
                                  ├─→ jobs.state    (compacted GUI state)
                                  ├─→ agent.events  (audit)
                                  ├─→ Phoenix       (OpenTelemetry)
                                  └─→ RustFS        (optional artifacts)

GUI ──→ jobs.in, jobs.state, agent.events
```

## Included

- A disposable **goose** microVM per job with the **goosetown** orchestrator,
  worker, writer, reviewer, and local-researcher skills
- An **LLM gateway** that holds the provider key and issues a short-lived token
  to every job, so jobs never see the real credential
- A browser GUI for submitting jobs and watching state, results, and audit
  events
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
- A model provider key for the gateway (`LLM_GATEWAY_UPSTREAM_API_KEY`, or the
  `OPENROUTER_API_KEY`/`OPENAI_API_KEY` fallback)
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
   LLM_GATEWAY_SECRET=replace-with-openssl-rand-hex-32
   LLM_GATEWAY_UPSTREAM_URL=https://openrouter.ai/api/v1
   LLM_GATEWAY_UPSTREAM_API_KEY=your-openrouter-key
   ```

2. Install the microVM runtime and start the rootless Podman API:

   ```bash
   dnf install crun-krun
   systemctl --user enable --now podman.socket
   ```

   Rootful Podman breaks libkrun's TSI networking on this setup. Use the rootless
   socket.

3. Build the goose job image into the **Podman** storage. This is separate from
   the Docker build of the worker and GUI:

   ```bash
   ./scripts/build-goose-image.sh
   ```

4. Start the stack:

   ```bash
   docker compose --profile storage up -d --build
   ```

5. Open the GUI:

   <http://localhost:8090>

The GUI is published on `127.0.0.1` only. It submits to `jobs.in` and reads the
state and audit topics. It has no local database.

### Command line

```bash
# Submit a job
docker compose run --rm jobctl submit "Say hello"

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
```

Redpanda Console runs at <http://localhost:8080>. Phoenix runs at
<http://localhost:6006>.

## How a job runs

1. The GUI or CLI writes a JSON job to `jobs.in` and a `queued` state to
   `jobs.state`.
2. The worker consumes `jobs.in`, publishes `running`, and prepares a unique
   directory under `GOOSE_JOB_WORKSPACE_ROOT`.
3. Referenced input files are downloaded from RustFS into `inputs/`.
4. The worker mints a short-lived signed token for the job and starts a goose
   microVM through the rootless Podman API. The job directory is bind mounted at
   `/workspace`, the prompt at `/jobboot`, and the persistent skills at
   `/workspace/.claude/skills`. The microVM receives `OPENAI_HOST` pointing at
   the LLM gateway and the token, never the provider key.
5. Inside the microVM, the entrypoint seeds the workspace with goosetown
   (`AGENTS.md`, the `goose` wrapper, `gtwall`, and the role skills) and runs:

   ```bash
   ./goose run --no-session -q --output-format json \
     --with-builtin developer --max-turns "$GOOSE_MAX_TURNS" \
     -i /jobboot/main-prompt.txt
   ```

6. goose talks the OpenAI protocol to the gateway. The gateway verifies the
   job token, replaces it with the real provider key, and streams the upstream
   response back to the microVM.
7. The goosetown orchestrator decomposes the job, dispatches parallel subagents
   through goose's `delegate`/`load` tools, coordinates them over `gtwall`, and
   writes result files to `outputs/`.
8. The worker parses the structured goose output, copies `outputs/` back,
   optionally runs a short skill-improvement pass, publishes `jobs.out` and the
   final `jobs.state`, uploads artifacts, and commits the Kafka offset.
9. Audit events go to `agent.events`; traces go to Phoenix when enabled.

The microVM never talks to Kafka. The worker is the only Kafka participant.

## LLM gateway and provider keys

The `llm-gateway` service is an OpenAI-compatible pass-through proxy between the
job microVMs and the model provider.

- The real provider key is configured only on the gateway
  (`LLM_GATEWAY_UPSTREAM_API_KEY`, or `OPENROUTER_API_KEY`/`OPENAI_API_KEY` as a
  fallback). It is not passed to the worker, the job, or the GUI.
- For every job the worker mints an HMAC-signed token bound to the `job_id` and
  the job deadline. The token is valid only through the gateway and expires
  shortly after the job budget. The job can reuse it until then, but cannot
  mint new tokens without `LLM_GATEWAY_SECRET`.
- The gateway accepts `Authorization: Bearer <job-token>`, verifies the
  signature and expiry, and forwards the request to the configured upstream
  with the real provider credential. It streams SSE responses without
  buffering, so goose sees the provider's normal streaming behavior.
- The job uses `GOOSE_PROVIDER=openai` and `OPENAI_HOST=<LLM_GATEWAY_URL>`. The
  gateway maps the OpenAI paths `/v1/...` onto the configured upstream base URL.
- The gateway logs the job ID, method, path, status, and duration. It never logs
  the job token or the provider key.

Configure the upstream with an OpenAI-compatible base URL (the same value you
would pass as `base_url` to the OpenAI SDK):

| Provider | `LLM_GATEWAY_UPSTREAM_URL` |
|---|---|
| OpenRouter | `https://openrouter.ai/api/v1` |
| OpenAI | `https://api.openai.com/v1` |
| Groq | `https://api.groq.com/openai/v1` |
| DeepSeek | `https://api.deepseek.com/v1` |
| Anthropic (OpenAI compatibility) | `https://api.anthropic.com/v1` |
| Gemini (OpenAI compatibility) | `https://generativelanguage.googleapis.com/v1beta/openai` |
| Ollama on the host | `http://host.docker.internal:11434/v1` |

If the provider does not use `Authorization: Bearer`, set
`LLM_GATEWAY_UPSTREAM_AUTH_HEADER` and `LLM_GATEWAY_UPSTREAM_AUTH_SCHEME` (for
example `api-key` with an empty scheme for Azure OpenAI).

The gateway is published on `${LLM_GATEWAY_BIND:-0.0.0.0}:${LLM_GATEWAY_PORT}`
because libkrun's TSI networking reaches host services through a host address,
not the host loopback. The per-job token is the access control. The listener is
plain HTTP, so keep it on a trusted network or put it behind a TLS reverse
proxy. `LLM_GATEWAY_SECRET` must be identical in the worker and the gateway;
changing it invalidates all outstanding job tokens.

Health check:

```bash
curl -fsS http://localhost:8091/healthz
```

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

- Debian bookworm slim, with bash, git, jq, Python 3, and ripgrep
- Pinned goose `1.57.0` release, verified with SHA-256 at build time
- No `curl`, `wget`, or other network clients in the final image
- goosetown template and skills under `/opt/goosetown`

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

## Why Docker and Podman

The control plane runs under Docker Compose; the job microVMs run under Podman.

- **Podman runs the microVMs.** Only Podman's `crun` krun runtime can start a
  libkrun microVM, selected through the `run.oci.handler=krun` annotation.
  Docker's runc has no krun handler.
- **Docker runs the control plane.** The worker, GUI, Redpanda, RustFS, Phoenix,
  and the socket proxy are ordinary containers.
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
| Filesystem | Disposable job directory, tmpfs `/opt/data`, no host paths beyond the bind mounts |
| Network | Reaches the LLM gateway, which forwards to the model provider. The Compose services and the Podman API are not reachable: the proxy listens on the worker's loopback, and `scripts/host-firewall.sh` blocks the Docker ranges |
| Credentials | The real provider key stays in the gateway. The job gets a short-lived signed token bound to its `job_id` |
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
- **General internet egress.** The image has no common network clients, and the
  gateway removes the provider key from the job, but the microVM can still
  create its own network connections. For stricter isolation, block direct
  egress from the rootless Podman UID and allow only the gateway host port.
- **Gateway token.** The job holds a bearer token until it expires. A malicious
  job can reuse that token for its own gateway requests during the job window.
  The gateway does not enforce a per-job request or token budget.
- **Gateway transport.** The gateway listener is plain HTTP. Keep it on a
  trusted network or terminate TLS in front of it.

For untrusted input, run:

```bash
sudo scripts/host-firewall.sh
```

This blocks the Podman user from the Docker bridge ranges (Redpanda, RustFS,
Phoenix, the worker, the GUI) while leaving the public internet and the gateway
reachable. Use `sudo scripts/host-firewall.sh --remove` to undo it. To restrict
the gateway listener to one host interface, set `LLM_GATEWAY_BIND`.

## Traceability and Phoenix

Every job receives a `trace_id`, propagated through Kafka headers and connected
to the worker and goose spans. Audit events include:

- `job.received` and `job.completed`
- `agent.completed`
- `goose.started` and `goose.completed`
- `goose.reflect`
- `artifact.stored` and `artifact.failed`

Phoenix displays the trace. Prompts, responses, and tool input/output are not
captured by default. Set `PHOENIX_CAPTURE_CONTENT=true` if that content may be
stored locally. Phoenix is optional; its ports are bound to `127.0.0.1`.

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

Submit a job with an input file:

```bash
docker compose run --rm jobctl submit \
  "Read the input file and summarize it" \
  --input-file jobs/example.txt
```

The bucket must already contain the file.

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

- The worker waits for Redpanda and the Podman API with exponential backoff
  before consuming jobs. It verifies the goose image and the skills directory.
- Redpanda, Console, RustFS, the Podman proxy, Phoenix, the worker, and the GUI
  have restart policies.
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
goose_job/          job image, goosetown template and skills
scripts/            build-goose-image.sh, host-firewall.sh, poc-krun.sh
tests/              unit tests
```
