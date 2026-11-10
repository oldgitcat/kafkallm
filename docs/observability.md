# Observability with MLflow

[Back to the main README](../README.md)

## Observability: MLflow

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

Every span carries the OpenTelemetry `session.id` attribute set to the job ID.
MLflow maps that to `mlflow.trace.session`, so the **Sessions** view groups the
whole job (submit, worker, agent, microVM) under one session per job. A restarted
job gets a new `job_id` and therefore its own session. Audit events such as
`job.received`, `goose.started`, and `goose.reflect` are also recorded as span
events, so they appear in the trace timeline without a separate query.

Goose's live activity is recorded as span events on `goose.job.vm.run`:
thinking, tool calls, tool results, assistant text, and token usage. The trace
timeline therefore shows what the agent did, not just the final result. Events
are truncated and capped per span. Set `TELEMETRY_CAPTURE_CONTENT=true` to also
attach the prompt and result as span inputs/outputs; otherwise the Input/Output
columns stay empty.

One trace per job includes the LLM calls as child spans:

- The worker starts the `goose.job.vm.run` trace and a per-job allowlisting
  proxy is created with its W3C `traceparent` and the `job.id`.
- The proxy injects `traceparent` and `x-kafkallm-job-id` into every gateway
  request, replacing any client-supplied values. agentgateway continues that
  trace, so its `POST /v1/chat/completions` and upstream spans are children of
  the job trace.
- Both the worker and agentgateway export to the same MLflow experiment
  (`MLFLOW_EXPERIMENT_ID`), so the **Traces** tab shows one trace per job with
  `job.submit`, `job.handle`, `agent.run`, `goose.job.vm.run`, and the individual
  LLM requests. The **Sessions** view still groups it by the `job_id`.
- Standalone playground requests have no job traceparent. They create their own
  traces in the same experiment; filter with
  `service.name = 'agent-worker'` to show one trace per job, or
  `service.name = 'agentgateway'` to inspect individual LLM requests.
- Older installations may still have an `agentgateway` experiment from the
  earlier split-experiment layout. It is no longer used and can be deleted.

MLflow is configured with:

```dotenv
MLFLOW_ENABLED=true
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_OTLP_HOST=mlflow
MLFLOW_OTLP_PORT=5000
MLFLOW_EXPERIMENT_ID=0
```

Prompts, responses, and tool input/output are not captured by default because
they can contain sensitive data. Set `TELEMETRY_CAPTURE_CONTENT=true` to include
them in the traces.

Trace export happens in the worker/control plane, which can reach MLflow over
the Compose network. The goose microVM keeps its strict egress isolation: it
does not export traces directly and reaches only the allowlisting proxy.

If you run the CLI on the host instead of `podman compose run --rm jobctl`, set
`MLFLOW_TRACKING_URI=http://localhost:5000`. The Compose name `mlflow` is not
resolvable from the host, so the `job.submit` span would be dropped and the
trace would stay `IN_PROGRESS` in the MLflow UI. GUI submissions are unaffected.


