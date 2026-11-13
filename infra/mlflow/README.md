# MLflow

MLflow 3.18 is the trace and observability backend. The service is defined in
the top-level `compose.yaml`.

- UI/API: <http://localhost:5000>
- Backend store: SQLite at `/mlflow/mlflow.db`
- Artifact root: `/mlflow/artifacts`
- Data volume: `mlflow-data`
- OTLP/HTTP ingestion is enabled for the worker, gateway, and job traces

The GUI and worker send OpenTelemetry spans to MLflow. Disable tracing with
`MLFLOW_ENABLED=false` if you do not need it.
