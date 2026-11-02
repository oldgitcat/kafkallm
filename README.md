# Hermes

Hermes is a small Kafka/Redpanda job queue where every job is executed by an isolated
[Hermes Agent](https://github.com/NousResearch/hermes-agent) runtime. Redpanda carries jobs,
results, and audit events. There is no PostgreSQL and no scheduler.

```text
Producer/CLI ──→ Redpanda jobs.in ──→ Hermes job worker
                                         │
                                         │ POST /v1/runs
                                         ▼
                                  Hermes Agent container
                                  ├─ agent loop and tools
                                  └─ shared job workspace

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
- Redpanda topics `jobs.in`, `jobs.out`, and `agent.events`
- Kafka worker with manual offset commits and a bounded startup wait
- Shared workspace between the Kafka worker and Hermes Agent
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
`OPENAI_API_KEY`, or `HERMES_PROVIDER=anthropic` with `ANTHROPIC_API_KEY`. If no provider is
configured, Hermes falls back to its own profile settings in the `hermes-data` volume; without
either, jobs fail with Hermes' "not connected to any AI provider" message.

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
| `hermes` | The worker submits each job to the Hermes Agent `/v1/runs` API. This is the production path. |
| `local` | The worker uses the built-in Python harness and `LLM_PROVIDER`. Intended for tests and debugging. |

The Hermes API is internal to the Compose network and is not published on a host port.
`HERMES_API_KEY` authenticates the worker against it. Change the default before exposing the
stack beyond a trusted local machine.

Compose always starts the Hermes Agent container. In `local` mode it is unused; to avoid the
extra container, start only the services you need:

```bash
docker compose up -d redpanda agent-worker
```

If you prefer to configure Hermes interactively instead of passing provider/model per request,
run its setup inside the persistent Hermes data volume:

```bash
docker compose exec --user 10001 hermes-agent hermes model
```

The picker is interactive and stores the choice in the persistent `hermes-data` volume.

## How jobs are executed

1. The producer writes a JSON job to `jobs.in`.
2. The worker validates it and prepares a unique directory under the shared `/workspace`.
3. Referenced input files are downloaded from RustFS into `inputs/`.
4. The worker submits a Hermes run with an idempotency key derived from `run_id`.
5. Hermes executes the instruction with its own tools and writes result files under `outputs/`.
6. The worker polls the Hermes run, publishes the final answer to `jobs.out`, uploads artifacts,
   and commits the Kafka offset.
7. Audit events are written to `agent.events`.

Hermes has its own broad tool registry and agent loop. The Kafka worker does not reimplement
the LLM loop. Hermes admits up to ten concurrent runs by default; when that limit is reached it
returns HTTP 429 and the worker retries with backoff until the job deadline.

The worker does not copy individual Hermes tool calls into `agent.events`; it publishes
job-level and run-level events only. Full tool progress remains available through Hermes' run
event stream (`GET /v1/runs/{run_id}/events`), its session store, and the agent container logs.

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

The `agent-workspace` volume is mounted at `/workspace` in both the Kafka worker and the Hermes
Agent container. The worker creates a separate run directory with `inputs/` and `outputs/` for
each job. The prompt gives Hermes the exact absolute paths.

The worker runs as UID/GID `10001`. The Hermes container is configured with the same UID/GID so
files written by Hermes are readable and uploadable by the worker.

`/workspace` is a shared trust boundary, not a per-job security sandbox. Every Hermes run can
read the directories of other runs in the same container, and anyone who holds `HERMES_API_KEY`
can execute tools in that container. The worker only uploads files below the current job's
`outputs/` directory. Run a separate Hermes Agent container per trust domain if stronger
isolation is required.

The workspace path is deterministic for `job_id` / `run_id`, which keeps the Hermes request
fingerprint stable so a redelivered message reuses the original idempotent run. The directory is
removed when the job finishes.

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
`max_steps` applies to `AGENT_RUNTIME=local`; Hermes manages its own agent turn limits.

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

Switch back to `AGENT_RUNTIME=hermes` for real agent execution.

## Multiple workers

```bash
docker compose up -d --scale agent-worker=2
```

Workers share the `agent-workers` consumer group. Each worker processes one job at a time.

## Operations and guarantees

- The worker waits for Redpanda and Hermes Agent with exponential backoff before consuming jobs.
- Redpanda, Console, RustFS, Hermes Agent, Phoenix, and the worker have restart policies.
- The Kafka offset is committed only after the result is written to `jobs.out`.
- Processing is **at-least-once**. A job can run again after a crash.
- Hermes requests use `run_id` as the `Idempotency-Key`, so retries within Hermes' retention
  window are deduplicated by the agent runtime.
- Audit delivery is asynchronous and best effort and does not change job status.
- Topics have seven days of retention by default (`KAFKA_TOPIC_RETENTION_MS`).
- The Compose configuration is for local development. Redpanda has no SASL/TLS here.
- The Hermes API server has full access to Hermes tools, including terminal commands inside its
  container. Keep `HERMES_API_KEY` secret and do not expose port `8642`.

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

The unit tests use mocked Hermes HTTP responses and require neither Docker nor Redpanda.
