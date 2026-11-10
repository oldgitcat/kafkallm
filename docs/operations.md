# Operations, testing, and troubleshooting

[Back to the main README](../README.md)

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
`podman compose up -d --scale agent-worker=2`. Do not set both higher than the
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
- stderr from the main run (learning-pass logs are not streamed)
- token usage when goose completes

The panel polls the `GET /api/jobs/<job_id>/logs?after=<sequence>` endpoint
every two seconds and keeps a bounded ring buffer per job. Raw log records are
also available with `kafkallm logs`. Job logs expire with the normal topic
retention (`KAFKA_TOPIC_RETENTION_MS`, seven days by default).

When the orchestrator delegates, the panel shows two extra records:
`agent.delegated` (role and instructions) and `agent.returned` (completion,
duration, and the delegate's error or result when Goose streams one). The
subagent's own thinking and tool calls run in a separate process and are not
part of the main stream, so these events identify which role ran and how long
it took. They are also mirrored to the MLflow span events.


## Multiple workers

```bash
podman compose up -d --scale agent-worker=2
```

Workers share the `agent-workers` consumer group. Each worker can run
`WORKER_CONCURRENCY` jobs in parallel, and multiple worker containers add more
slots. Concurrent learning passes merge individual skill files with
last-writer-wins semantics; existing files are never deleted.


## Operations and guarantees

- The worker waits for Kafka, agentgateway readiness, the Podman API, the
  job network, the allowlisting proxy, and the goose image before consuming
  jobs.
- Kafka, the Kafka UI, RustFS, the Podman proxy, MLflow, the worker, and the GUI
  have restart policies.
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
podman compose down
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

It checks Python, Podman and Podman Compose, the rootless socket, KVM, the job
network, the job image, the required `.env` values, the rendered gateway config,
the gateway, the GUI, and the selected LLM provider. Use
`./scripts/doctor.sh --offline` to skip network checks.

Common failures:

| Symptom | Fix |
|---|---|
| `KVM` fails | Enable virtualization in the BIOS and make `/dev/kvm` writable |
| `Podman socket` fails | `systemctl --user enable --now podman.socket` |
| `Job network` warns | Run `make network` on the host, not inside a container |
| `Goose job image` warns | Run `make images` |
| Worker waits for the gateway | Check `LLM_UPSTREAM_PROVIDER`/`LLM_UPSTREAM_API_KEY` in `.env`, run `make gateway-config`, then `make restart` |
| Gateway rejects the virtual key | Rerun `make setup`, then `make restart` |
| `Gateway config` warns | Run `make gateway-config` after changing provider settings |
| Provider key rejected | Create a new key and update `LLM_UPSTREAM_API_KEY` |
| Ollama unreachable from the gateway | Use `LLM_UPSTREAM_BASE_URL=http://host.containers.internal:11434/v1` |
| `podman compose config` fails | Fix the reported `.env` or `compose.yaml` value |

Logs:

```bash
make logs
podman compose logs -f agent-worker
podman compose logs -f gui
podman compose logs -f llm-gateway
```


## Repository layout

```text
src/kafkallm/       worker, GUI, job store, doctor, Kafka, state, agents,
                    sandboxes, goose runtime, output parsing, models
goose_job/          job image, goosetown template, skills, config and tool examples
agents/             built-in agent profile defaults
agentgateway/       LLM gateway template and generated provider config
scripts/            setup, doctor, config render, image builds, network init, sandbox helpers
tests/              unit tests
.github/workflows/  CI (lint, tests, Compose validation)
Makefile            one-command setup, diagnostics, build, and run targets
```


