# Hermes

Hermes is a small, containerized LLM job processor using **Redpanda for jobs, results, and audit events**. It intentionally has no PostgreSQL and no scheduler. Arize Phoenix is optional as a graphical trace viewer, not as a job store.

> **Rename note:** This branch renames the application from `kafkallm` to Hermes. Kafka topic names remain unchanged. The Compose project, Python package, CLI command, and default Phoenix project are now Hermes, so Docker creates separate `hermes_*` volumes. Existing `kafkallm` containers and volumes are left untouched and must be migrated separately if needed.

```text
Producer/CLI → Redpanda jobs.in → isolated agent worker
                                      ├─→ Redpanda jobs.out
                                      ├─→ Redpanda agent.events
                                      ├─→ OpenTelemetry → Phoenix
                                      └─→ optional RustFS for files
```

## Included

- Redpanda with the topics `jobs.in`, `jobs.out`, and `agent.events`
- Python worker with manual Kafka offset commits
- Correlated traces for submit, worker, agent, LLM, and tool calls
- Local Arize Phoenix UI with a persistent SQLite volume
- Agent harness with timeout, step limit, and a separate workspace per run
- Tools for reading, writing, and listing workspace files
- OpenAI-compatible LLM integration with tool calling
- Echo provider for tests without an LLM or API key
- Optional S3-compatible RustFS container for input and output files
- Redpanda Console and a CLI for submitting jobs and reading results

## Quick start

Requirements: Docker and Docker Compose.

```bash
cp .env.example .env
docker compose up -d --build
```

The default worker uses `LLM_PROVIDER=echo`. This lets you test the entire pipeline without an external LLM service.

Submit a job:

```bash
docker compose run --rm jobctl submit "Say hello"
```

Read results:

```bash
docker compose run --rm jobctl results --from-beginning --idle-timeout 3
```

View worker logs:

```bash
docker compose logs -f agent-worker
```

Redpanda Console is available at <http://localhost:8080>, where you can inspect topics and individual messages. The Phoenix UI is available at <http://localhost:6006>.

## Traceability and Phoenix

Every job receives a `trace_id`. It is propagated through Kafka headers and connects the submission, worker, agent, and all LLM and tool calls. The worker also writes structured audit events to `agent.events`:

- `job.received` and `job.completed`
- `agent.completed`
- `llm.completed`
- `tool.completed`
- `artifact.stored` and `artifact.failed`

The events contain duration, status, errors, token usage, and relationships through `parent_event_id`. You can read audit messages without Phoenix:

```bash
docker compose run --rm jobctl events --from-beginning --idle-timeout 3
```

Phoenix displays the same distributed execution as a span tree. Prompts, LLM responses, and tool input/output are **not** captured by default. If this content may be stored locally in Phoenix, set the following in `.env`:

```dotenv
PHOENIX_CAPTURE_CONTENT=true
```

Then recreate the worker and CLI images as needed. Audit events still contain only metadata with this setting; content is sent exclusively to Phoenix. The Phoenix container stores data in a persistent volume, and the job pipeline does not depend on Phoenix.

`PHOENIX_ENABLED=false` disables OTLP export from the worker and CLI. `PHOENIX_UI_PORT` and `OTLP_GRPC_PORT` can be used to change occupied host ports. The Compose setup runs Phoenix without authentication and therefore binds it exclusively to `127.0.0.1`. Network or production deployments must configure authentication and TLS separately.

## Using a real LLM

Set values such as the following in `.env`:

```dotenv
LLM_PROVIDER=openai
LLM_MODEL=your-model
LLM_API_KEY=your-api-key
# Optional for Ollama, vLLM, or another OpenAI-compatible endpoint:
LLM_BASE_URL=
```

Then recreate the worker:

```bash
docker compose up -d --build --force-recreate agent-worker
```

For a local OpenAI-compatible endpoint without authentication, `LLM_API_KEY` may remain empty. The endpoint must be reachable from the worker container.

The agent can use these tools:

| Tool | Purpose |
|---|---|
| `read_workspace_file` | Read a UTF-8 file from the workspace |
| `write_workspace_file` | Write a text file; result files belong under `outputs/` |
| `list_workspace_files` | List existing workspace files |

There is intentionally no shell tool and no host access.

## Enabling RustFS and files

```bash
docker compose --profile storage up -d --build
```

- S3 API: <http://localhost:9000>
- RustFS Console: <http://localhost:9001/rustfs/console/>
- Local default account: `rustfs`
- Local default secret: `hermes-secret`

These credentials are for local development only and should be changed in `.env`. If the ports are already in use, adjust `RUSTFS_API_PORT` and `RUSTFS_CONSOLE_PORT` in `.env`.

The S3 endpoint must be configured in `.env`, for example:

```dotenv
S3_ENDPOINT_URL=http://rustfs:9000
```

Input files are provided as S3 references:

```bash
docker compose run --rm jobctl submit \
  "Read the input file and summarize it" \
  --input-file jobs/example.txt
```

The bucket must already contain the file. The worker downloads it into `inputs/`. Files created by the agent under `outputs/` are uploaded to RustFS by the harness. The result in `jobs.out` then contains the bucket and key.

A missing or unreachable artifact store no longer turns an otherwise successful agent run into a failed job. The job remains `succeeded`, but the files are missing and `artifact_error` describes the upload failure. Uploads are attempted three times, and partially uploaded files are cleaned up after a final failure.

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

`job_id` and `run_id` are generated automatically as UUIDs. Messages use `job_id` as the Kafka key.

A result includes:

- `status`: `succeeded` or `failed`
- `result` or `error`
- `artifacts`: uploaded files
- `artifact_error`: optional failure while storing generated files; does not change job status
- `trace_id`, `worker_id`, timestamps, steps, and token usage

## Multiple workers

```bash
docker compose up -d --scale agent-worker=2
```

Workers share the `agent-workers` consumer group. Each worker processes one job at a time. Topics are created with three partitions by default.

## Operations and guarantees

- On startup, the worker waits up to `KAFKA_STARTUP_TIMEOUT_SECONDS` (default: 300 seconds) for Redpanda with exponential backoff instead of entering a crash loop.
- Redpanda, Console, RustFS, and the worker have restart policies. The stack recovers automatically after a machine or Docker restart.
- The Kafka offset is committed only after the result has been successfully written to `jobs.out`.
- Processing is **at-least-once**. A job may run again after a crash.
- Audit events are queued asynchronously. An audit or Phoenix failure changes neither job status nor result.
- Topics have a default retention of seven days, configurable through `KAFKA_TOPIC_RETENTION_MS`. Increase it for longer audit retention.
- Phoenix is only an optional trace viewer; failed OTLP exports do not change any job result.
- The worker runs as non-root with a read-only root filesystem, no Linux capabilities, and no host mounts.
- The Compose setup is intended for development and testing. Redpanda has no SASL/TLS configuration here.

Stop without deleting data volumes:

```bash
docker compose down
```

## Local tests

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

The unit tests require neither Docker nor Redpanda.
