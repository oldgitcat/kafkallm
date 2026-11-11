from __future__ import annotations

import logging
import shutil
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from kafkallm.audit import AuditLogger, JobAudit
from kafkallm.goose_job_runtime import (
    GooseJobRunner,
    JobCancelledError,
    WorkerShutdown,
)
from kafkallm.models import AgentResult, FileRef, Job, JobResult, utc_now
from kafkallm.safe_fs import iter_regular_files
from kafkallm.sandboxes import SandboxStore
from kafkallm.settings import Settings
from kafkallm.storage import S3Storage
from kafkallm.telemetry import (
    current_trace_id,
    get_tracer,
    new_trace_id,
    set_content,
    set_error,
)

logger = logging.getLogger(__name__)

# Ten years. Used internally for "unlimited" jobs so deadline arithmetic and
# timeout calculations remain finite without imposing a practical limit.
_UNLIMITED_TIMEOUT_SECONDS = 10 * 365 * 24 * 60 * 60


@contextmanager
def _job_workspace(root: Path, job: Job) -> Iterator[Path]:
    workspace = root / f"{job.job_id}-{job.run_id}"
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        yield workspace
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


class Harness:
    def __init__(
        self,
        settings: Settings,
        audit: AuditLogger | None = None,
        sandboxes: SandboxStore | None = None,
    ) -> None:
        self.settings = settings
        self.audit = audit
        self.goose = GooseJobRunner(settings, sandboxes=sandboxes)
        self._storage: S3Storage | None = None

    def wait_ready(self) -> None:
        self.goose.wait_ready()

    def cancel(self) -> None:
        self.goose.cancel_active()

    def close(self) -> None:
        self.goose.close()

    def _get_storage(self) -> S3Storage:
        if self._storage is None:
            self._storage = S3Storage(self.settings)
        return self._storage

    def _limit_result(
        self,
        agent_result: AgentResult,
        outputs: Path,
        job: Job,
    ) -> AgentResult:
        """Keep Kafka messages bounded and preserve large results as artifacts."""
        if not job.unlimited:
            return agent_result
        limit = max(1, self.settings.max_result_bytes)
        encoded = agent_result.text.encode("utf-8")
        if len(encoded) <= limit:
            return agent_result
        full_path = outputs / "result-full.md"
        full_path.write_text(agent_result.text, encoding="utf-8")
        truncated = encoded[:limit].decode("utf-8", errors="ignore")
        return AgentResult(
            text=(
                truncated + "\n\n[Result truncated for the Kafka state message. "
                "The full result was written to result-full.md and is available "
                "as an artifact when S3 storage is configured.]"
            ),
            steps=agent_result.steps,
            usage=agent_result.usage,
        )

    def process(
        self,
        job: Job,
        job_audit: JobAudit | None = None,
        heartbeat: Callable[[], bool] | None = None,
        log: Callable[[str, str], None] | None = None,
    ) -> JobResult:
        trace_id = job.trace_id or current_trace_id() or new_trace_id()
        job = job.model_copy(update={"trace_id": trace_id})
        if job_audit is None and self.audit is not None:
            job_audit = self.audit.for_job(job)
        started_at = utc_now()
        run_started = time.perf_counter()
        if job.unlimited:
            timeout = _UNLIMITED_TIMEOUT_SECONDS
        else:
            timeout = min(job.timeout_seconds, self.settings.max_job_timeout_seconds)
        deadline = time.monotonic() + timeout
        workspace_root = Path(self.settings.workspace_root)
        workspace_root.mkdir(parents=True, exist_ok=True)
        steps = 0
        usage: dict[str, int] = {}

        with get_tracer().start_as_current_span(
            "agent.run",
            attributes={
                "openinference.span.kind": "AGENT",
                "agent.name": self.settings.agent_name,
                "agent.runtime": "goose-job",
                "job.id": job.job_id,
                "job.run_id": job.run_id,
                "kafkallm.trace_id": trace_id,
                "session.id": job.job_id,
                "worker.id": self.settings.worker_id,
            },
        ) as span:
            set_content(span, "input.value", job.instruction)
            try:
                if job.unlimited and not self.settings.allow_unlimited_jobs:
                    raise RuntimeError(
                        "Unlimited jobs are disabled. Set ALLOW_UNLIMITED_JOBS=true "
                        "on the worker and GUI."
                    )
                with _job_workspace(workspace_root, job) as root:
                    inputs = root / "inputs"
                    outputs = root / "outputs"
                    inputs.mkdir(exist_ok=True)
                    outputs.mkdir(exist_ok=True)

                    if job.input_files:
                        storage = self._get_storage()
                        for index, reference in enumerate(job.input_files):
                            if time.monotonic() > deadline:
                                raise TimeoutError(
                                    "The job timeout was reached while downloading."
                                )
                            filename = Path(reference.key).name or f"input-{index}"
                            storage.download(
                                reference, inputs / f"{index:02d}-{filename}"
                            )

                    agent_result: AgentResult = self.goose.run(
                        job=job,
                        workspace=root,
                        deadline=deadline,
                        audit=job_audit,
                        heartbeat=heartbeat,
                        log=log,
                    )
                    agent_result = self._limit_result(agent_result, outputs, job)
                    steps = agent_result.steps
                    usage = agent_result.usage

                    output_files = [path for path, _ in iter_regular_files(outputs)]
                    artifacts: list[FileRef] = []
                    artifact_error: str | None = None
                    if output_files:
                        if not self.settings.storage_configured:
                            artifact_error = (
                                f"{len(output_files)} result file(s) were not stored "
                                "because no S3 endpoint is configured."
                            )
                        else:
                            try:
                                artifacts = self._get_storage().upload_outputs(
                                    outputs=outputs,
                                    job_id=job.job_id,
                                    run_id=job.run_id,
                                    deadline=deadline,
                                    max_files=(
                                        0
                                        if job.unlimited
                                        else self.settings.max_output_files
                                    ),
                                )
                            except Exception as exc:
                                logger.exception(
                                    "Artifact upload failed for job %s",
                                    job.job_id,
                                )
                                artifact_error = str(exc)[:4000]

                        if artifact_error:
                            if job_audit:
                                job_audit.emit(
                                    "artifact.failed",
                                    "failed",
                                    attributes={"file_count": len(output_files)},
                                    error=artifact_error,
                                )
                        else:
                            for artifact in artifacts:
                                if job_audit:
                                    job_audit.emit(
                                        "artifact.stored",
                                        "succeeded",
                                        attributes={
                                            "bucket": artifact.bucket,
                                            "key": artifact.key,
                                        },
                                    )

                span.set_attribute("agent.steps", steps)
                span.set_attribute("agent.artifact_count", len(artifacts))
                if artifact_error:
                    span.set_attribute("agent.artifact_error", artifact_error)
                for key, value in usage.items():
                    metric = key.replace("_tokens", "")
                    span.set_attribute(f"llm.token_count.{metric}", value)
                set_content(span, "output.value", agent_result.text)
                if job_audit:
                    job_audit.emit(
                        "agent.completed",
                        "succeeded",
                        duration_ms=round(
                            (time.perf_counter() - run_started) * 1000, 3
                        ),
                        attributes={
                            "steps": steps,
                            "usage": usage,
                            "artifact_count": len(artifacts),
                            "artifact_error": artifact_error,
                        },
                    )
                return JobResult(
                    job_id=job.job_id,
                    run_id=job.run_id,
                    status="succeeded",
                    trace_id=trace_id,
                    result=agent_result.text,
                    artifacts=artifacts,
                    artifact_error=artifact_error,
                    worker_id=self.settings.worker_id,
                    started_at=started_at,
                    finished_at=utc_now(),
                    steps=steps,
                    usage=usage,
                )
            except Exception as exc:
                set_error(span, exc)
                if isinstance(exc, (JobCancelledError, WorkerShutdown)):
                    logger.info(
                        "Job %s / run %s stopped: %s",
                        job.job_id,
                        job.run_id,
                        exc,
                    )
                else:
                    logger.exception("Job %s / run %s failed", job.job_id, job.run_id)
                if job_audit:
                    job_audit.emit(
                        "agent.completed",
                        "failed",
                        duration_ms=round(
                            (time.perf_counter() - run_started) * 1000, 3
                        ),
                        attributes={"steps": steps, "usage": usage},
                        error=str(exc),
                    )
                return JobResult(
                    job_id=job.job_id,
                    run_id=job.run_id,
                    status="failed",
                    trace_id=trace_id,
                    cancelled=isinstance(exc, JobCancelledError),
                    error=str(exc)[:4000],
                    worker_id=self.settings.worker_id,
                    started_at=started_at,
                    finished_at=utc_now(),
                    steps=steps,
                    usage=usage,
                )
