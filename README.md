# Hermes

Hermes is a small Kafka/Redpanda job queue where every job is executed by an isolated
[Hermes Agent](https://github.com/NousResearch/hermes-agent) runtime. Redpanda carries jobs,
results, and audit events. There is no PostgreSQL and no scheduler.

```text
Producer/CLI ──→ Redpanda jobs.in ──→ Hermes job worker
                                         │
                                         │ Podman API via socket proxy
                                         ▼
                               disposable Hermes microVM (KVM)
                               ├─ agent loop and tools
                               ├─ tmpfs /opt/data (memory off)
                               ├─ per-job /workspace bind mount
                               └─ shared skills directory

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

- A disposable Hermes Agent **microVM** per job, with a shared, persistent skills directory
- Redpanda topics `jobs.in`, `jobs.out`, and `agent.events`
- Kafka worker with manual offset commits and a bounded startup wait
- Correlated traces for submission, worker, agent, and the Hermes run
- Structured Kafka audit events
- Arize Phoenix UI with a persistent SQLite volume
- Optional S3-compatible RustFS container for input files and result artifacts
- A shared Hermes gateway runtime and a built-in Echo harness for tests
- Redpanda Console and a CLI for submitting jobs and reading results

## Requirements

- Docker and Docker Compose
- KVM (`/dev/kvm`) plus Podman with the `krun` runtime (`crun-krun`)
- A rootless Podman API socket: `systemctl --user enable --now podman.socket`
- A model provider configured through `HERMES_PROVIDER` / `HERMES_MODEL` plus the matching key

The official Hermes Agent image is large because it includes the complete Hermes runtime.

## Quick start

```bash
cp .env.example .env
```

On Fedora, install the microVM runtime and start the rootless Podman API:

```bash
dnf install crun-krun
systemctl --user enable --now podman.socket
```

Rootful Podman breaks libkrun's TSI networking on this setup, so the job
microVMs get no outbound network there. The rootless socket is the supported
path. The worker and the microVMs share `/var/lib/hermes`, which is made
world-writable because rootless Podman maps the job's uid into a subuid range.

Configure a Hermes provider:

```dotenv
AGENT_RUNTIME=hermes-job
HERMES_PROVIDER=openrouter
HERMES_MODEL=nvidia/nemotron-3.5-lightning:free
OPENROUTER_API_KEY=your-openrouter-key
```

Each job runs in a fresh microVM, so it always starts with an empty profile: set
`HERMES_PROVIDER`, `HERMES_MODEL`, and the matching key. Learned skills persist in the shared
skills directory; memory is disabled.

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
| `hermes-job` | The worker starts one disposable Hermes **microVM** per job through Podman's `krun` runtime. This is the default and the primary path. |
| `hermes` | The worker submits each job to a shared Hermes Agent `/v1/runs` API. No per-job isolation. |
| `local` | The worker uses the built-in Python harness and `LLM_PROVIDER`. Intended for tests and debugging. |

The `hermes` and `local` modes exist as fallbacks for debugging. The `hermes` gateway is internal
to the Compose network and is not published on a host port; `HERMES_API_KEY` authenticates the
worker against it.

To avoid starting the gateway and its init services when only `hermes-job` is used, start the
services you need:

```bash
docker compose up -d redpanda podman-proxy agent-worker
```

If you prefer to configure Hermes interactively instead of passing provider/model per request,
run its setup inside the persistent Hermes data volume:

```bash
docker compose exec --user 10001 hermes-agent hermes model
```

The picker is interactive and stores the choice in the persistent `hermes-data` volume.

## MicroVM job containers

With `AGENT_RUNTIME=hermes-job`, every job runs in its own KVM microVM through
[libkrun](https://github.com/libkrun/libkrun). The worker creates the microVM through the
rootless Podman API (fronted by the internal `podman-proxy`), waits for it, reads the
result from the shared workspace directory, copies `outputs/` back, and removes the
microVM and its job directory. Each job gets its own kernel, so a kernel exploit no longer
reaches the host.

Per-job layout:

| Mount | Type | Purpose |
|---|---|---|
| `/workspace` | bind mount `hermes-job-<run_id>` | `inputs/`, `outputs/`, and the result files; removed after collection |
| `/opt/data` | tmpfs (`HERMES_JOB_TMPFS_SIZE`, default `1g`) | sessions, config, cache; discarded with the microVM |
| `/opt/data/skills` | bind mount `HERMES_JOB_SKILLS_DIR` | bundled and learned skills shared across jobs |

The job bundle (script, prompt, reflect prompt) is bind mounted read-only, and the results are
written back into the shared workspace directory. Nothing is read from the microVM's filesystem
afterwards.

The microVM costs more than a container: each job reserves `HERMES_JOB_RAM_MIB` of memory and
boots a guest kernel, which takes about three seconds for the Hermes image.

### Learning: skills and reflect

The job disables the built-in memory stores and Hermes' automatic background review:

- `memory.memory_enabled=false`
- `memory.user_profile_enabled=false`
- `auxiliary.background_review.enabled=false`

The automatic review cannot be relied on in a one-shot process because `hermes chat --oneshot`
exits with `os._exit`, which also stops Hermes' daemon review thread. Instead, after a successful
main run, the worker starts a second one-shot run with `--resume <session_id>` and a fixed review
prompt. That reflect run can inspect the completed session and update or create reusable skills
through `skill_manage`. It runs before the microVM is removed, so the shared skills directory sees
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
`agent.run_budget_seconds` is set in the microVM's temporary `config.yaml`, and `/usr/bin/timeout`
provides a hard stop.

### Concurrency and cleanup

Multiple workers and jobs can run at the same time. The skills directory is shared and has no
locking, so concurrent reflect runs use last-writer-wins semantics. Use one worker or separate
skills directories per trust domain when deterministic skill updates matter.

The worker removes the microVM and its job directory in a `finally` block. At startup it also
removes microVM containers and job directories labeled `hermes.vm=1` that are older than
`MAX_JOB_TIMEOUT_SECONDS + 300` seconds. A job that is redelivered after a crash gets a new
microVM; the job directory is derived from `run_id`, and an existing directory with that name is
replaced first.

### Isolation model

Each job runs in its own KVM microVM. What that boundary covers:

| Property | Value |
|---|---|
| Kernel | Its own guest kernel, separate from the host |
| Privileges | The agent process runs as uid 10001 with no effective capabilities |
| Privilege escalation | `no-new-privileges:true` blocks the setuid binaries in the image |
| Resources | `HERMES_JOB_RAM_MIB` (default `2048`), `HERMES_JOB_CPUS` (default `2`), `HERMES_JOB_PIDS_LIMIT` (default `512`) |
| Filesystem | Disposable job directory, tmpfs `/opt/data`, no host paths beyond the two bind mounts |
| Network | TSI by default (the VMM's network context); `HERMES_JOB_USE_PASST=true` for firewall control |
| Lifetime | The microVM and its job directory are removed after the job |

What the boundary does **not** cover:

- **The hypervisor.** libkrun and KVM run on the host, so a VMM bug is still a host compromise.
  A microVM raises the bar, but it is not a bulletproof boundary.
- **Root-equivalent Podman API.** The worker creates microVMs through the socket proxy, which is
  equivalent to root on the host. The proxy sits on the internal `control` network that only the
  worker joins, and the socket is never published, but a compromised worker still means a
  compromised host.
- **Shared skills directory.** `HERMES_JOB_SKILLS_DIR` is writable by every job. A job can poison
  the skills that later jobs load. Use one skills directory per trust domain.
- **Internet egress.** Jobs can reach the internet and can therefore exfiltrate whatever they can
  read. Use `HERMES_JOB_USE_PASST=true` and a firewall, or an egress proxy.
- **Provider credentials.** The provider keys are passed into every job.

For untrusted input, the practical next step is to run the Podman service on a separate host or
VM. The job microVMs then cannot reach the host daemon at all, and a VMM bug stays contained in
that machine. `scripts/poc-krun.sh` boots the Hermes image in a microVM and reports the guest
kernel, the start-up time, and the prerequisites.

## How jobs are executed

1. The producer writes a JSON job to `jobs.in`.
2. The worker validates it and prepares a unique directory under its staging workspace.
3. Referenced input files are downloaded from RustFS into `inputs/`.
4. The selected runtime executes the job:
   - `hermes-job`: create a microVM and a job directory, copy `inputs/` and the prompt into it,
     start the main one-shot run, then run the optional reflect pass.
   - `hermes`: submit a run to the shared Hermes Agent API with an idempotency key derived from
     `run_id`.
   - `local`: run the built-in Python agent harness.
5. Hermes executes the instruction with its own tools and writes result files under `outputs/`.
6. The worker collects the final answer and outputs, optionally runs reflect, publishes the result
   to `jobs.out`, uploads artifacts, and commits the Kafka offset.
7. Audit events are written to `agent.events`.

Hermes has its own broad tool registry and agent loop. The Kafka worker does not reimplement the
LLM loop. In `hermes-job` mode concurrency is bounded by the host memory, KVM, and the number of
workers.

The worker does not copy individual Hermes tool calls into `agent.events`; it publishes job-level
and run-level events only. In `hermes-job` mode the session is discarded with the microVM, so use
the `hermes.started` / `hermes.completed` / `hermes.reflect` events and the job result.

Unattended jobs cannot answer interactive approval prompts. If a run enters
`waiting_for_approval`, the worker stops it and fails the job with an explicit error. The job
passes `--yolo`, so dangerous command approvals are bypassed for trusted workloads.

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

The worker runs as UID/GID `10001`. It stages each job under its `/workspace` directory and
copies the inputs into the job directory under `HERMES_JOB_WORKSPACE_ROOT`, which is a host
directory shared between the worker container and the microVMs. The job bundle (script, prompt,
reflect prompt) is bind mounted read-only, and the results are written back into the same
directory. After the job, the worker copies `outputs/` back to its staging workspace for upload
and deletes the job directory.

The shared skills directory is `HERMES_JOB_SKILLS_DIR`. Both the worker and every microVM see the
same host directory, so learned skills persist across jobs.

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

Switch back to `AGENT_RUNTIME=hermes-job` for real agent execution.

## Multiple workers

```bash
docker compose up -d --scale agent-worker=2
```

Workers share the `agent-workers` consumer group. Each worker processes one job at a time.

## Operations and guarantees

- The worker waits for Redpanda and the Podman API with exponential backoff before consuming
  jobs. It verifies the pinned Hermes image (pulling it into the Podman storage if missing) and
  the skills directory.
- Redpanda, Console, RustFS, Hermes Agent, the Podman proxy, Phoenix, and the worker have restart
  policies.
- The Kafka offset is committed only after the result is written to `jobs.out`.
- Processing is **at-least-once**. A job can run again after a crash.
- In `hermes` mode, requests use `run_id` as the `Idempotency-Key`, so retries within Hermes'
  retention window are deduplicated by the agent runtime. In `hermes-job` mode a redelivered job
  starts a new microVM, so side effects must be idempotent.
- Audit delivery is asynchronous and best effort and does not change job status.
- Topics have seven days of retention by default (`KAFKA_TOPIC_RETENTION_MS`).
- The Compose configuration is for local development. Redpanda has no SASL/TLS here.
- The Podman socket proxy fronts the rootless Podman API. It is attached
  to the internal `control` network that only the worker joins, and the socket is never published.
- In `hermes` mode, the Hermes API server has full access to Hermes tools, including terminal
  commands inside its container. Keep `HERMES_API_KEY` secret and do not expose port `8642`.
- In `hermes-job` mode, each job has its own microVM but still shares the skills directory. Run
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

The unit tests use mocked Hermes HTTP responses and a fake Podman client. They require neither a
Podman daemon nor Redpanda.
