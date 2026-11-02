# Hermes

Hermes is a small Kafka/Redpanda job queue where every job is executed by an isolated
[Hermes Agent](https://github.com/NousResearch/hermes-agent) runtime. Redpanda carries jobs,
results, and audit events. There is no PostgreSQL and no scheduler.

```text
Producer/CLI ──→ Redpanda jobs.in ──→ Hermes job worker
                                         │
              ┌──────────────────────────┴──────────────────────────┐
              │ AGENT_RUNTIME=hermes                                │ AGENT_RUNTIME=hermes-job
              │ POST /v1/runs                                       │ Docker API via socket proxy
              ▼                                                     ▼
       Hermes Agent container                              disposable Hermes container
       ├─ agent loop and tools                             ├─ agent loop and tools
       └─ shared job workspace                             ├─ tmpfs /opt/data (memory off)
                                                           ├─ per-job /workspace volume
                                                           └─ shared hermes-skills volume

Hermes job worker ──→ Redpanda jobs.out
                  ├─→ Redpanda agent.events
                  ├─→ OpenTelemetry → Phoenix
                  └─→ optional RustFS for result files
```

> **Rename note:** This branch renames the application from `kafkallm` to Hermes. Kafka topic
> names remain unchanged. The Compose project, Python package, CLI command, and default Phoenix
> project are now Hermes, so Docker creates separate `hermes_*` volumes. Existing `kafkallm`
> containers and volumes are left untouched.

## Included

- Hermes Agent running as its own container with its official HTTP API server
- Optional disposable per-job Hermes containers with a persistent skills volume
- Redpanda topics `jobs.in`, `jobs.out`, and `agent.events`
- Kafka worker with manual offset commits and a bounded startup wait
- Shared workspace for the gateway runtime and per-job workspace volumes for `hermes-job`
- Correlated traces for submission, worker, agent, Hermes API run, LLM, and local tool calls
- Structured Kafka audit events
- Arize Phoenix UI with a persistent SQLite volume
- Optional S3-compatible RustFS container for input files and result artifacts
- OpenAI-compatible local fallback runtime for tests without provider credentials
- Redpanda Console and a CLI for submitting jobs and reading results

## Requirements

- Docker and Docker Compose
- A model provider configured for Hermes Agent, either through per-run
  `HERMES_PROVIDER` / `HERMES_MODEL` settings plus provider credentials, or through a configured
  Hermes profile in its data volume
- Docker socket access for `AGENT_RUNTIME=hermes-job`; the included Compose stack provides an
  internal socket proxy

The official Hermes Agent image is large because it includes the complete Hermes runtime.

## Quick start

```bash
cp .env.example .env
```

Configure a Hermes provider. For example:

```dotenv
AGENT_RUNTIME=hermes
HERMES_API_KEY=change-me-local-dev
HERMES_PROVIDER=openrouter
HERMES_MODEL=anthropic/claude-sonnet-4.6
OPENROUTER_API_KEY=your-openrouter-key
```

OpenAI and Anthropic credentials are passed through as well: use `HERMES_PROVIDER=openai` with
`OPENAI_API_KEY`, or `HERMES_PROVIDER=anthropic` with `ANTHROPIC_API_KEY`. In `hermes` mode, if
no provider is configured, Hermes falls back to its own profile settings in the `hermes-data`
volume; without either, jobs fail with Hermes' "not connected to any AI provider" message. In
`hermes-job` mode each container starts with an empty profile, so set `HERMES_PROVIDER`,
`HERMES_MODEL`, and the matching key.

For one disposable Hermes container per job, set:

```dotenv
AGENT_RUNTIME=hermes-job
```

The same provider credentials are passed to each job container. Learned skills persist in the
`hermes-skills` volume; memory is disabled. See [Ephemeral job containers](#ephemeral-job-containers).

Then start the stack:

```bash
docker compose --profile storage up -d --build
```

Submit a job:

```bash
docker compose run --rm jobctl submit "Say hello"
```

Read results:

```bash
docker compose run --rm jobctl results --from-beginning --idle-timeout 5
```

Read audit events:

```bash
docker compose run --rm jobctl events --from-beginning --idle-timeout 5
```

Worker logs:

```bash
docker compose logs -f agent-worker
```

Hermes Agent logs:

```bash
docker compose logs -f hermes-agent
```

Redpanda Console is available at <http://localhost:8080>. The Phoenix UI runs at
<http://localhost:6006>.

## Runtime modes

`AGENT_RUNTIME` selects the execution engine:

| Value | Behavior |
|---|---|
| `hermes` | The worker submits each job to the shared Hermes Agent `/v1/runs` API. Skills and memory live in `hermes-data`. This is the default. |
| `hermes-job` | The worker starts a disposable Hermes container per job through the Docker socket proxy. Memory is off; learned skills persist in `hermes-skills`. |
| `local` | The worker uses the built-in Python harness and `LLM_PROVIDER`. Intended for tests and debugging. |

The Hermes API is internal to the Compose network and is not published on a host port.
`HERMES_API_KEY` authenticates the worker against it. Change the default before exposing the
stack beyond a trusted local machine.

Compose always starts the Hermes Agent container. In `local` mode it is unused; to avoid the
extra container, start only the services you need:

```bash
docker compose up -d redpanda agent-worker
```

In `hermes-job` mode the gateway container is still useful: it provides the pinned image used by
job containers, seeds bundled skills, and remains available as the `hermes` fallback. The worker
talks to Docker only through the internal `docker-proxy` service.

If you prefer to configure Hermes interactively instead of passing provider/model per request,
run its setup inside the persistent Hermes data volume:

```bash
docker compose exec --user 10001 hermes-agent hermes model
```

The picker is interactive and stores the choice in the persistent `hermes-data` volume.

## Ephemeral job containers

With `AGENT_RUNTIME=hermes-job`, every job runs in a fresh Hermes container. The worker creates
the container through the Docker SDK and the internal `docker-proxy`, waits for it, reads the
`stream-json` result, copies `outputs/` back to its own staging workspace, and removes the
container and its workspace volume.

Per-job layout:

| Mount | Type | Purpose |
|---|---|---|
| `/workspace` | named volume `hermes-job-<run_id>` | `inputs/` and `outputs/`; removed after collection |
| `/opt/data` | tmpfs (`HERMES_JOB_TMPFS_SIZE`, default `1g`) | sessions, config, cache; discarded with the container |
| `/opt/data/skills` | named volume `hermes-skills` | bundled and learned skills shared across jobs |

Job containers use the default Docker bridge network. They are not attached to the Compose
network, so they can reach model providers on the internet but cannot resolve `redpanda`,
`hermes-agent`, `phoenix`, or `docker-proxy`.

### Learning: skills and reflect

The job container disables the built-in memory stores and Hermes' automatic background review:

- `memory.memory_enabled=false`
- `memory.user_profile_enabled=false`
- `auxiliary.background_review.enabled=false`

The automatic review cannot be relied on in a one-shot process because `hermes chat --oneshot`
exits with `os._exit`, which also stops Hermes' daemon review thread. Instead, after a successful
main run, the worker starts a second one-shot run with `--resume <session_id>` and a fixed review
prompt. That reflect run can inspect the completed session and update or create reusable skills
through `skill_manage`. It runs before the container is removed, so the shared skills volume sees
the result.

Reflect is best-effort. A reflect failure is reported as a `hermes.reflect` audit event with
status `failed`, but the job remains `succeeded`. Set `HERMES_JOB_REFLECT_ENABLED=false` to skip
it, or tune it with `HERMES_JOB_REFLECT_TIMEOUT_SECONDS` (default `300`). The main run is limited
by `HERMES_JOB_MAX_TURNS` (default `100`); the reflect run resumes that session and therefore
inherits the same turn policy.

Provider errors are reported the same way. A rate-limited or quota-exhausted model (for example
an OpenRouter free tier without credits) fails only the reflect pass; the job result stays
`succeeded`.

Each job starts with the full time budget from `timeout_seconds`, capped by
`MAX_JOB_TIMEOUT_SECONDS`. The main run may use the budget minus a reserve of 25 seconds and at
least a quarter of the remaining time, which is kept for the reflect pass. The reflect pass then
uses the time the main run left unused, capped by `HERMES_JOB_REFLECT_TIMEOUT_SECONDS`.
`agent.run_budget_seconds` is set in the container's temporary `config.yaml`, and
`/usr/bin/timeout` provides a hard stop.

### Concurrency and cleanup

Multiple workers and jobs can run at the same time. The `hermes-skills` volume is shared and has
no locking, so concurrent reflect runs use last-writer-wins semantics. Use one worker or separate
skills volumes per trust domain when deterministic skill updates matter.

The worker removes the job container and workspace volume in a `finally` block. At startup it
also removes job containers and volumes labeled `hermes.job=1` that are older than
`MAX_JOB_TIMEOUT_SECONDS + 300` seconds. A job that is redelivered after a crash gets a new
container; the volume name is derived from `run_id`, and an existing volume with that name is
replaced first.

### Isolation model

Each job runs in its own container. What that boundary covers:

| Property | Value |
|---|---|
| Namespaces | Separate PID, mount, network, IPC, and user namespaces |
| Privileges | The agent process runs as uid 10001 with no effective capabilities |
| Privilege escalation | `no-new-privileges:true` blocks the setuid binaries in the image |
| Syscalls | Docker's default seccomp profile |
| Resources | `HERMES_JOB_MEMORY_LIMIT` (default `4g`), `HERMES_JOB_PIDS_LIMIT` (default `512`), `HERMES_JOB_CPUS` (default `2`) |
| Filesystem | Disposable workspace volume, tmpfs `/opt/data`, no host paths |
| Network | Default bridge: internet for the model provider, but no route to `redpanda`, `hermes-agent`, `phoenix`, or `docker-proxy` |
| Lifetime | The container and its workspace volume are removed after the job |

What the boundary does **not** cover:

- **Shared kernel.** The container shares the host kernel, so a kernel or driver bug becomes a
  host compromise. A container is a process boundary, not a hardware boundary.
- **Root-equivalent Docker API.** The worker creates containers through the socket proxy, which
  is equivalent to root on the host. The proxy sits on the internal `control` network that only
  the worker joins, and port `2375` is never published, but a compromised worker still means a
  compromised host.
- **Shared skills volume.** `hermes-skills` is writable by every job and by the gateway. A job can
  poison the skills that later jobs load. Use one skills volume per trust domain.
- **Internet egress.** Jobs can reach the internet and can therefore exfiltrate whatever they
  can read. Put an egress proxy in front of the provider if that matters.
- **Provider credentials.** The provider keys are passed into every job container.

### Stronger isolation

Containers are enough for jobs you trust. For untrusted input, raise the boundary:

| Option | Isolation | Cost | Notes |
|---|---|---|---|
| Containers (default) | Namespaces, seccomp, capabilities | Low | What this setup does today |
| gVisor (`runsc`) | User-space kernel between the app and the host kernel | Low to medium | Drop-in Docker runtime, small overhead, a few syscalls unsupported |
| libkrun via `podman --runtime=krun` | Real microVM with its own kernel | Medium | Keeps the container workflow, see below |
| Kata Containers | Real VM per container on hardware virtualization | Medium | ~1-2 s startup, ~150 MB per VM, virtiofs for mounts |
| Firecracker | Micro-VM per job | Medium to high | Fastest cold start (~125 ms), but needs a custom runtime instead of the plain Docker API |
| Separate Docker daemon | Real VM or rootless daemon for job containers | Medium | Keeps the container API, but a job escape stays inside that daemon |

The most effective step for untrusted jobs is the last row: point `DOCKER_HOST` at a second
Docker daemon that runs in a VM or as a rootless user daemon. Job containers then cannot touch
the host daemon at all, and a container escape remains contained in that daemon.

#### libkrun in detail

[libkrun](https://github.com/libkrun/libkrun) starts a KVM microVM with its own kernel. Through
the `krun` mode of `crun` it keeps the container workflow: same image, same commands, but the
workload runs behind a hypervisor.

```bash
dnf install crun-krun
podman run --runtime=krun --rm nousresearch/hermes-agent hermes --version
```

That fits this project better than the other options because the worker already speaks the
Docker API, and Podman exposes a Docker-compatible API: the same `containers.create`,
`exec_run`, `put_archive`, and label-filtered listings work against Podman. The runtime code
would only need a different socket target, plus `krun.cpus` and `krun.ram_mib` annotations and a
decision about where Podman runs.

Open points to settle before using it for jobs:

- **Where Podman runs.** A host service keeps the worker simple but lets it bind host paths. A
  Podman inside the worker container needs `/dev/kvm` and user namespaces, which weakens the
  worker's own confinement.
- **Cost per job.** The Hermes image is 3 GB, and a microVM needs its own memory
  (`krun.ram_mib`). Measure the start-up time before rolling it out.
- **Egress control.** The default TSI networking proxies guest sockets in the VMM's network
  context and bypasses host firewall rules. `krun.use_passt=1` restores firewall control.
- **Annotations.** The Docker API cannot set OCI annotations, so `krun.cpus` and `krun.ram_mib`
  need a `.krun_vm.json` in a derived image or Podman's native API.

`scripts/poc-krun.sh` boots the Hermes image in a microVM and reports the guest kernel, the
start-up time, and the prerequisites.

## How jobs are executed

1. The producer writes a JSON job to `jobs.in`.
2. The worker validates it and prepares a unique directory under its staging workspace.
3. Referenced input files are downloaded from RustFS into `inputs/`.
4. The selected runtime executes the job:
   - `hermes`: submit a run to the shared Hermes Agent API with an idempotency key derived from
     `run_id`.
   - `hermes-job`: create a disposable container and workspace volume, copy `inputs/` and the
     prompt into it, start the main one-shot run, then run the optional reflect pass.
   - `local`: run the built-in Python agent harness.
5. Hermes executes the instruction with its own tools and writes result files under `outputs/`.
6. The worker collects the final answer and outputs, optionally runs reflect, publishes the result
   to `jobs.out`, uploads artifacts, and commits the Kafka offset.
7. Audit events are written to `agent.events`.

Hermes has its own broad tool registry and agent loop. The Kafka worker does not reimplement
the LLM loop. In `hermes` mode Hermes admits up to ten concurrent runs by default; when that
limit is reached it returns HTTP 429 and the worker retries with backoff until the job deadline.
In `hermes-job` mode concurrency is bounded by the Docker host and the number of workers.

The worker does not copy individual Hermes tool calls into `agent.events`; it publishes
job-level and run-level events only. In `hermes` mode full tool progress remains available
through Hermes' run event stream (`GET /v1/runs/{run_id}/events`), its session store, and the
agent container logs. In `hermes-job` mode the session is discarded with the container, so use
the `hermes.started` / `hermes.completed` / `hermes.reflect` events and the job result.

Unattended jobs cannot answer interactive approval prompts. If a run enters
`waiting_for_approval`, the worker stops it and fails the job with an explicit error. For trusted
workloads, set the persistent approval policy in the Hermes profile instead:

```bash
docker compose exec --user 10001 hermes-agent hermes config set approvals.mode off
```

Use `manual` (the default) or `smart` when commands must still be reviewed.

## Traceability and Phoenix

Every job receives a `trace_id`. It is propagated through Kafka headers and connects job
submission, worker processing, the agent span, and the Hermes API run.

The worker writes these audit events to `agent.events`:

- `job.received` and `job.completed`
- `agent.completed`
- `hermes.started` and `hermes.completed`
- `hermes.reflect` in `hermes-job` mode
- `artifact.stored` and `artifact.failed`
- `llm.completed` and `tool.completed` in local runtime mode

Phoenix displays the distributed trace as a span tree. Prompts, LLM responses, and tool
input/output are **not** captured by default. If this content may be stored locally in Phoenix,
set:

```dotenv
PHOENIX_CAPTURE_CONTENT=true
```

Phoenix is optional and does not affect job success. Its ports are bound to `127.0.0.1`.
`PHOENIX_ENABLED=false` disables OTLP export.

## Hermes workspace and artifacts

In `hermes` mode, the `agent-workspace` volume is mounted at `/workspace` in both the Kafka
worker and the Hermes Agent container. In `local` mode only the worker uses it. The worker creates
a separate run directory with `inputs/` and `outputs/` for each job, and the prompt gives Hermes
the exact absolute paths.

In `hermes-job` mode, the worker uses `/workspace` only for staging. Each job container receives
its own disposable `hermes-job-<run_id>` volume at `/workspace`; the worker copies inputs in and
copies `outputs/` back before deleting the volume.

The worker runs as UID/GID `10001`. The shared Hermes container is configured with the same
UID/GID so files written by Hermes are readable and uploadable by the worker. Job containers
start as root so they can prepare the mounted workspace and normalize ownership of the shared
skills volume to `10001:10001` before they exit.

In `hermes` mode, `/workspace` is a shared trust boundary, not a per-job security sandbox. Every
Hermes run can read the directories of other runs in the same container, and anyone who holds
`HERMES_API_KEY` can execute tools in that container. The worker only uploads files below the
current job's `outputs/` directory. Use `hermes-job` mode for per-job process isolation, or run a
separate Hermes Agent container per trust domain.

For `hermes` mode, the workspace path is deterministic for `job_id` / `run_id`, which keeps the
request fingerprint stable so a redelivered message reuses the original idempotent run. In
`hermes-job` mode, the workspace volume name is derived from `run_id`; an existing volume with
that name is replaced before the new container starts. Both are removed when the job finishes.

Artifacts are uploaded only from `outputs/`. A missing or unreachable artifact store does not
turn a successful agent run into a failed job. The job remains `succeeded`, with an
`artifact_error` describing the failed upload. Uploads are attempted three times, and partially
uploaded files are cleaned up after a final failure.

## RustFS and input files

Start with the storage profile:

```bash
docker compose --profile storage up -d --build
```

The S3 endpoint must be configured in `.env`:

```dotenv
S3_ENDPOINT_URL=http://rustfs:9000
```

Local RustFS defaults:

- S3 API: <http://localhost:9000>
- Console: <http://localhost:9001/rustfs/console/>
- Account: `rustfs`
- Secret: `hermes-secret`

These credentials are for local development only.

Submit a job with an input file:

```bash
docker compose run --rm jobctl submit \
  "Read the input file and summarize it" \
  --input-file jobs/example.txt
```

The bucket must already contain the file.

## Job format

```json
{
  "job_id": "optionally-set-by-producer",
  "run_id": "optionally-set-by-producer",
  "trace_id": "generated-automatically-via-opentelemetry",
  "type": "llm_task",
  "instruction": "Analyze the input data",
  "input": {" arbitrary": "JSON data" },
  "input_files": [],
  "timeout_seconds": 300,
  "max_steps": 6
}
```

`job_id` and `run_id` are generated as UUIDs if omitted. Messages use `job_id` as the Kafka key.
`max_steps` applies to `AGENT_RUNTIME=local`; Hermes manages its own agent turn limits. In
`hermes-job` mode, `HERMES_JOB_MAX_TURNS` controls the main run. The reflect run resumes the same
session and inherits its turn policy; its time budget is controlled by
`HERMES_JOB_REFLECT_TIMEOUT_SECONDS`.

A result includes:

- `status`: `succeeded` or `failed`
- `result` or `error`
- `artifacts`: uploaded files
- `artifact_error`: optional upload failure; does not change job status
- `trace_id`, `worker_id`, timestamps, steps, and token usage

## Local Echo mode

For a quick end-to-end test without an LLM provider, temporarily set:

```dotenv
AGENT_RUNTIME=local
LLM_PROVIDER=echo
```

Then recreate the worker:

```bash
docker compose up -d --build --force-recreate agent-worker
```

Switch back to `AGENT_RUNTIME=hermes` or `AGENT_RUNTIME=hermes-job` for real agent execution.

## Multiple workers

```bash
docker compose up -d --scale agent-worker=2
```

Workers share the `agent-workers` consumer group. Each worker processes one job at a time.

## Operations and guarantees

- The worker waits for Redpanda and the selected runtime with exponential backoff before consuming
  jobs. In `hermes-job` mode it also pings Docker through `docker-proxy` and verifies the pinned
  image and skills volume.
- Redpanda, Console, RustFS, Hermes Agent, the Docker proxy, Phoenix, and the worker have restart
  policies.
- The Kafka offset is committed only after the result is written to `jobs.out`.
- Processing is **at-least-once**. A job can run again after a crash.
- In `hermes` mode, requests use `run_id` as the `Idempotency-Key`, so retries within Hermes'
  retention window are deduplicated by the agent runtime. In `hermes-job` mode a redelivered job
  starts a new container, so side effects must be idempotent.
- Audit delivery is asynchronous and best effort and does not change job status.
- Topics have seven days of retention by default (`KAFKA_TOPIC_RETENTION_MS`).
- The Compose configuration is for local development. Redpanda has no SASL/TLS here.
- The Docker socket proxy grants root-equivalent Docker access. It is attached to the internal
  `control` network that only the worker joins, and port `2375` is never published.
- In `hermes` mode, the Hermes API server has full access to Hermes tools, including terminal
  commands inside its container. Keep `HERMES_API_KEY` secret and do not expose port `8642`.
- In `hermes-job` mode, each job has its own container but still shares the skills volume. Run
  only trusted jobs and inputs. See [Isolation model](#isolation-model).

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

The unit tests use mocked Hermes HTTP responses and a fake Docker client. They require neither a
Docker daemon nor Redpanda.
